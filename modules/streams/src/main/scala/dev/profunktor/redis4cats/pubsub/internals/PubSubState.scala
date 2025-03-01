/*
 * Copyright 2018-2021 ProfunKtor
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package dev.profunktor.redis4cats.pubsub.internals

import cats.{ Applicative, ApplicativeThrow, Monad }
import cats.syntax.all._
import cats.effect.kernel.{ Async, Concurrent, Deferred, MonadCancelThrow, Ref, Resource, Sync }
import cats.effect.std.{ AtomicCell, Dispatcher, MapRef }
import dev.profunktor.redis4cats.data.{ RedisChannel, RedisPattern, RedisPatternEvent }
import dev.profunktor.redis4cats.effect.Log
import fs2.Stream
import fs2.concurrent.Topic
import io.lettuce.core.pubsub.{ RedisPubSubAdapter, RedisPubSubListener, StatefulRedisPubSubConnection }

/** We use `AtomicCell` instead of `Ref` because we need locking while side-effecting. */
private[pubsub] trait PubSubState[F[_], K, V] {
  def channelSubs: PubSubState.SubscriptionMap[F, RedisChannel[K], V]
  def patternSubs: PubSubState.SubscriptionMap[F, RedisPattern[K], RedisPatternEvent[K, V]]
}

private[pubsub] object PubSubState {

  trait SubscriptionMap[F[_], K, V] {
    def counts: F[Map[K, Long]]

    def subscribe(key: K)(create: Resource[F, Topic[F, V]]): Stream[F, V]

    def unsubscribe(key: K): F[Unit]

    private[PubSubState] def onMessage(key: K, message: V): F[Unit]
  }

  private object SubscriptionMap {
    def makeCell[F[_]: Concurrent, K, V]: F[SubscriptionMap[F, K, V]] =
      AtomicCell[F].of(Map.empty[K, Redis4CatsSubscription[F, V]]).map(fromCell[F, K, V])

    def fromCell[F[_]: MonadCancelThrow, K, V](
        cell: AtomicCell[F, Map[K, Redis4CatsSubscription[F, V]]]
    ): SubscriptionMap[F, K, V] =
      new SubscriptionMap[F, K, V] {
        override def counts: F[Map[K, Long]] =
          cell.get.map(_.iterator.map { case (k, v) => k -> v.subscribers }.toMap)

        override def subscribe(key: K)(create: Resource[F, Topic[F, V]]): Stream[F, V] =
          Stream.eval(addSubscription(key)(create)).flatMap(_.stream(remove(key)))

        private def addSubscription(key: K)(create: Resource[F, Topic[F, V]]): F[Redis4CatsSubscription[F, V]] =
          cell.evalModify { subscribers =>
            val getSubscription = subscribers.get(key) match {
              case Some(subscription) =>
                // We have an existing subscription, mark that it has one more subscriber.
                subscription.addSubscriber.pure[F]
              case None =>
                // No existing subscription, create a new one.
                create.allocated.map { case (topic, cleanup) =>
                  Redis4CatsSubscription(topic, subscribers = 1, cleanup)
                }
            }
            getSubscription.map(s => (subscribers.updated(key, s), s))
          }

        private def remove(key: K): F[Unit] =
          cell.evalUpdate { subscribers =>
            subscribers.get(key) match {
              case Some(sub) =>
                if (sub.isLastSubscriber) sub.cleanup.as(subscribers - key)
                else subscribers.updated(key, sub.removeSubscriber).pure
              case None =>
                // We were notified about stream termination but we don't have a subscription, this would be a bug
                subscribers.pure
            }
          }

        override def unsubscribe(key: K): F[Unit] =
          cell.get.map(_.get(key)).flatMap {
            // No subscription = nothing to do
            case None => Applicative[F].unit
            // `close` will terminate all streams, which will perform cleanup once the last stream
            // terminates.
            case Some(sub) => sub.topic.close.void
          }

        private[PubSubState] def onMessage(key: K, message: V): F[Unit] =
          cell.get.flatMap(_.get(key).traverse_(_.topic.publish1(message).void))
      }

    def fromShards[F[_]: Monad, K, V](shards: Vector[SubscriptionMap[F, K, V]]): SubscriptionMap[F, K, V] =
      new SubscriptionMap[F, K, V] {
        override def counts: F[Map[K, Long]] = shards.foldMapM(_.counts)

        override def subscribe(key: K)(create: Resource[F, Topic[F, V]]): Stream[F, V] =
          getKeyShard(key).subscribe(key)(create)

        override def unsubscribe(key: K): F[Unit] =
          getKeyShard(key).unsubscribe(key)

        private[PubSubState] override def onMessage(key: K, message: V): F[Unit] =
          getKeyShard(key).onMessage(key, message)

        private def getKeyShard(key: K): SubscriptionMap[F, K, V] = {
          val location = Math.abs(key.## % shards.size)
          shards(location)
        }
      }

    sealed trait SubscriptionState[F[_], V]
    object SubscriptionState {
      final case class Active[F[_], V](subscription: Redis4CatsSubscription[F, V]) extends SubscriptionState[F, V]
      final case class Starting[F[_], V](done: F[Unit]) extends SubscriptionState[F, V]
      final case class ShuttingDown[F[_], V](done: F[Unit]) extends SubscriptionState[F, V]
    }

    def makeRef[F[_]: Concurrent: Log, K, V]: F[SubscriptionMap[F, K, V]] =
      Ref[F].of(Map.empty[K, SubscriptionState[F, V]]).map(fromRef[F, K, V])

    def fromRef[F[_]: Concurrent: Log, K, V](
        ref: Ref[F, Map[K, SubscriptionState[F, V]]]
    ): SubscriptionMap[F, K, V] =
      new SubscriptionMap[F, K, V] {
        import SubscriptionState._

        private val mapRef = MapRef.fromSingleImmutableMapRef(ref)

        override def counts: F[Map[K, Long]] =
          ref.get.map(_.iterator.collect { case (k, Active(v)) => k -> v.subscribers }.toMap)

        override def subscribe(key: K)(create: Resource[F, Topic[F, V]]): Stream[F, V] =
          Stream.eval(addSubscription(key)(create)).flatMap(_.stream(remove(key)))

        private def addSubscription(key: K)(create: Resource[F, Topic[F, V]]): F[Redis4CatsSubscription[F, V]] =
          Deferred[F, Unit].flatMap { d =>
            ref.flatModifyFull[Redis4CatsSubscription[F, V]] { (poll, subscribers) =>
              subscribers.get(key) match {
                case Some(Active(subscription)) =>
                  // We have an existing subscription, mark that it has one more subscriber.
                  val newSubscription = subscription.addSubscriber
                  val log = Log[F].debug(
                    s"Returning existing subscription for $key, " +
                      s"subscribers: ${subscription.subscribers} -> ${newSubscription.subscribers}"
                  )
                  (subscribers.updated(key, Active(newSubscription)), log.as(newSubscription))
                case Some(ShuttingDown(wait)) =>
                  // an existing subscription is getting shut down, wait and try again
                  (subscribers, poll(wait) >> addSubscription(key)(create))
                case Some(Starting(wait)) =>
                  // an existing subscription is getting created, wait and try again
                  (subscribers, poll(wait) >> addSubscription(key)(create))
                case None =>
                  // No existing subscription, create a new one.
                  val start = create.allocated.flatMap { case (topic, cleanup) =>
                    val subscription = Redis4CatsSubscription(topic, subscribers = 1, cleanup)
                    mapRef(key).flatModify {
                      case Some(Starting(_)) => (Some(Active(subscription)), d.complete(()).as(subscription))
                      case _                 =>
                        // this would be a bug, we only expect a starting subscription
                        val action = cleanup >>
                          d.complete(()) >>
                          Log[F].error(
                            s"Subscription in unexpected state after creation, this is a bug in redis4cats!"
                          ) >>
                          ApplicativeThrow[F].raiseError[Redis4CatsSubscription[F, V]](
                            new IllegalStateException("Subscription could not be created after invalid state change")
                          )
                        (None, action)
                    }
                  }
                  val logged = Log[F].info(s"Creating subscription for $key") *> start <* Log[F].debug(
                    s"Created subscription for $key"
                  )
                  (subscribers.updated(key, Starting(d.get)), logged)
              }
            }
          }

        private def remove(key: K): F[Unit] =
          Deferred[F, Unit].flatMap { d =>
            mapRef(key).flatModify {
              case Some(Active(sub)) =>
                if (sub.isLastSubscriber) {
                  val cleanup = sub.cleanup >> mapRef(key).flatModify {
                    case Some(ShuttingDown(_)) => (None, d.complete(()).void)
                    case other =>
                      val action =
                        d.complete(()) >>
                          Log[F].error(
                            "Subscription in unexpected state after cleanup, this is a bug in redis4cats!"
                          )
                      (other, action)
                  }
                  (Some(ShuttingDown(d.get)), cleanup)
                } else (Some(Active(sub.removeSubscriber)), Applicative[F].unit)
              case other =>
                // `remove` is only called from `subscribe` after we have an active subscription,
                // so we shouldn't get a `remove` for `None` or `Starting`.
                // We can only end up in `ShuttingDown` after the last `remove` for a subscription
                // so we shouldn't get a `remove` for `ShuttingDown`.
                val log = Log[F].error(
                  s"We were notified about stream termination for $key but we don't have an active subscription, " +
                    s"this is a bug in redis4cats!"
                )
                (other, log)
            }
          }

        override def unsubscribe(key: K): F[Unit] =
          ref.get.map(_.get(key)).flatMap {
            // No subscription = nothing to do
            case None => Log[F].debug(s"Not unsubscribing from $key because we don't have a subscription")
            // Subscription already shutting down = nothing to do
            case Some(ShuttingDown(_)) => Applicative[F].unit
            // `close` will terminate all streams, which will perform cleanup once the last stream
            // terminates.
            case Some(Active(sub)) =>
              Log[F].info(s"Unsubscribing from $key with ${sub.subscribers} subscribers") >>
                sub.topic.close.void
            // wait until the subscription has started and unsubscribe
            case Some(Starting(wait)) => wait >> unsubscribe(key)
          }

        private[PubSubState] override def onMessage(key: K, message: V): F[Unit] =
          ref.get.flatMap(_.get(key).collect { case Active(s) => s }.traverse_(_.topic.publish1(message).void))
      }
  }

  def make[F[_]: Async: Log, K, V](
      shards: Option[Int],
      subConnection: StatefulRedisPubSubConnection[K, V]
  ): Resource[F, PubSubState[F, K, V]] =
    for {
      impl <- Resource.eval {
                shards.filter(_ > 1) match {
                  case None    => singleRef[F, K, V]
                  case Some(n) => sharded[F, K, V](n)
                }
              }
      dispatcher <- Dispatcher.parallel[F]
      _ <- Resource.make {
             val listener = impl.listener(dispatcher)
             Sync[F].delay(subConnection.addListener(listener)).as(listener)
           }(listener => Sync[F].delay(subConnection.removeListener(listener)))
    } yield impl

  // def single[F[_]: Concurrent, K, V]: F[PubSubStateImpl[F, K, V]] =
  //   for {
  //     channelSubs0 <- SubscriptionMap.makeCell[F, RedisChannel[K], V]
  //     patternSubs0 <- SubscriptionMap.makeCell[F, RedisPattern[K], RedisPatternEvent[K, V]]
  //   } yield new PubSubStateImpl[F, K, V](channelSubs0, patternSubs0)

  private def sharded[F[_]: Concurrent, K, V](number: Int): F[PubSubStateImpl[F, K, V]] = {
    assert(number > 1)
    for {
      channelShards <- SubscriptionMap.makeCell[F, RedisChannel[K], V].replicateA(number)
      patternShards <- SubscriptionMap.makeCell[F, RedisPattern[K], RedisPatternEvent[K, V]].replicateA(number)
    } yield new PubSubStateImpl[F, K, V](
      SubscriptionMap.fromShards(channelShards.toVector),
      SubscriptionMap.fromShards(patternShards.toVector)
    )
  }

  private def singleRef[F[_]: Concurrent: Log, K, V]: F[PubSubStateImpl[F, K, V]] =
    for {
      channelSubs0 <- SubscriptionMap.makeRef[F, RedisChannel[K], V]
      patternSubs0 <- SubscriptionMap.makeRef[F, RedisPattern[K], RedisPatternEvent[K, V]]
    } yield new PubSubStateImpl[F, K, V](channelSubs0, patternSubs0)

  private class PubSubStateImpl[F[_], K, V](
      override val channelSubs: PubSubState.SubscriptionMap[F, RedisChannel[K], V],
      override val patternSubs: PubSubState.SubscriptionMap[F, RedisPattern[K], RedisPatternEvent[K, V]]
  ) extends PubSubState[F, K, V] {
    def listener(dispatcher: Dispatcher[F]): RedisPubSubListener[K, V] =
      new RedisPubSubAdapter[K, V] {
        override def message(ch: K, msg: V): Unit =
          try
            dispatcher.unsafeRunSync(channelSubs.onMessage(RedisChannel(ch), msg))
          catch {
            case _: IllegalStateException => throw PubSubInternals.DispatcherAlreadyShutdown()
          }
        override def message(pattern: K, channel: K, message: V): Unit =
          try
            dispatcher.unsafeRunSync(
              patternSubs.onMessage(RedisPattern(pattern), RedisPatternEvent(pattern, channel, message))
            )
          catch {
            case _: IllegalStateException => throw PubSubInternals.DispatcherAlreadyShutdown()
          }
      }
  }
}
