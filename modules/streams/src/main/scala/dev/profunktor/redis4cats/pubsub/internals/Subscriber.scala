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

package dev.profunktor.redis4cats
package pubsub
package internals

import cats.effect.kernel._
import cats.effect.std.Dispatcher
import cats.syntax.all._
import dev.profunktor.redis4cats.data.{ RedisChannel, RedisPattern, RedisPatternEvent }
import dev.profunktor.redis4cats.effect.{ FutureLift, Log }
import fs2.Stream
import fs2.concurrent.Topic
import io.lettuce.core.pubsub.{ RedisPubSubListener, StatefulRedisPubSubConnection }

import cats.{ Applicative, ApplicativeThrow, Monad }
import cats.syntax.all._
import cats.effect.kernel.{ Async, Concurrent, Deferred, MonadCancelThrow, Ref, Resource, Sync }
import cats.effect.std.{ AtomicCell, Dispatcher, MapRef }
import dev.profunktor.redis4cats.data.{ RedisChannel, RedisPattern, RedisPatternEvent }
import dev.profunktor.redis4cats.effect.Log
import fs2.Stream
import fs2.concurrent.Topic
import io.lettuce.core.pubsub.{ RedisPubSubAdapter, RedisPubSubListener, StatefulRedisPubSubConnection }

private[pubsub] class Subscriber[F[_]: Async: FutureLift: Log, K, V] private (
    private val state: Subscriber.State[F, K, V],
    private val subConnection: StatefulRedisPubSubConnection[K, V]
) extends SubscribeCommands[F, Stream[F, *], K, V] {

  override def subscribe(channel: RedisChannel[K]): Stream[F, V] =
    Subscriber.subscribe(
      channel,
      state.channelSubs,
      subscribeToRedis = FutureLift[F].lift(subConnection.async().subscribe(channel.underlying)).void,
      unsubscribeFromRedis = FutureLift[F].lift(subConnection.async().unsubscribe(channel.underlying)).void
    )

  override def unsubscribe(channel: RedisChannel[K]): F[Unit] =
    state.channelSubs.unsubscribe(channel)

  override def psubscribe(
      pattern: RedisPattern[K]
  ): Stream[F, RedisPatternEvent[K, V]] =
    Subscriber.subscribe(
      pattern,
      state.patternSubs,
      subscribeToRedis = FutureLift[F].lift(subConnection.async().psubscribe(pattern.underlying)).void,
      unsubscribeFromRedis = FutureLift[F].lift(subConnection.async().punsubscribe(pattern.underlying)).void
    )

  override def punsubscribe(pattern: RedisPattern[K]): F[Unit] =
    state.patternSubs.unsubscribe(pattern)

  override def internalChannelSubscriptions: F[Map[RedisChannel[K], Long]] =
    state.channelSubs.counts

  override def internalPatternSubscriptions: F[Map[RedisPattern[K], Long]] =
    state.patternSubs.counts
}

object Subscriber {

  def make[F[_]: Async: FutureLift: Log, K, V](
      subConnection: StatefulRedisPubSubConnection[K, V]
  ): Resource[F, SubscribeCommands[F, Stream[F, *], K, V]] =
    for {
      dispatcher <- Dispatcher.parallel[F]
      state <- Resource.eval(State.fromRefs[F, K, V])
      _ <- Resource.make {
             val listener = listener(state, dispatcher)
             Sync[F].delay(subConnection.addListener(listener)).as(listener)
           }(listener => Sync[F].delay(subConnection.removeListener(listener)))
    } yield new Subscriber(state, subConnection)

  private def listener[F[_], K, V](
      state: State[F, K, V],
      dispatcher: Dispatcher[F]
  ): RedisPubSubListener[K, V] =
    new RedisPubSubAdapter[K, V] {
      override def message(ch: K, msg: V): Unit =
        try
          dispatcher.unsafeRunSync(state.channelSubs.onMessage(RedisChannel(ch), msg))
        catch {
          case _: IllegalStateException => throw PubSubInternals.DispatcherAlreadyShutdown()
        }
      override def message(pattern: K, channel: K, message: V): Unit =
        try
          dispatcher.unsafeRunSync(
            state.patternSubs.onMessage(RedisPattern(pattern), RedisPatternEvent(pattern, channel, message))
          )
        catch {
          case _: IllegalStateException => throw PubSubInternals.DispatcherAlreadyShutdown()
        }
    }

  private def subscribe[F[_]: Async: Log, TypedKey, SubValue, K, V](
      key: TypedKey,
      state: SubscriptionMap[F, TypedKey, SubValue],
      subConnection: StatefulRedisPubSubConnection[K, V],
      subscribeToRedis: F[Unit],
      unsubscribeFromRedis: F[Unit]
  ): Stream[F, SubValue] =
    state.subscribe(key) {
      for {
        _ <- Resource.eval(Log[F].info(s"Creating subscription for $key"))
        topic <- Resource.eval(Topic[F, SubValue])
        _ <- Resource.make(subscribeToRedis)(_ => unsubscribeFromRedis)
        _ <- Resource.eval(Log[F].debug(s"Created subscription for $key"))
      } yield topic
    }

  /** Stores an ongoing subscription.
    *
    * @param topic
    *   single-publisher, multiple-subscribers. The same topic is reused if `subscribe` is invoked more than once. The
    *   subscribers' streams are terminated when `None` is published.
    * @param subscribers
    *   subscriber count, when `subscribers` reaches 0 `cleanup` is called and `None` is published to the topic.
    */
  private final case class Redis4CatsSubscription[F[_], V](
      topic: Topic[F, V],
      subscribers: Long,
      cleanup: F[Unit]
  ) {
    assert(subscribers > 0, s"subscribers must be > 0, was $subscribers")

    def addSubscriber: Redis4CatsSubscription[F, V]    = copy(subscribers = subscribers + 1)
    def removeSubscriber: Redis4CatsSubscription[F, V] = copy(subscribers = subscribers - 1)
    def isLastSubscriber: Boolean                      = subscribers == 1

    def stream(onTermination: F[Unit])(
        implicit F: Applicative[F]
    ): fs2.Stream[F, V] =
      topic.subscribe(500).onFinalize(onTermination)
  }

  private final case class State[F[_], K, V](
      channelSubs: SubscriptionMap[F, RedisChannel[K], V],
      patternSubs: SubscriptionMap[F, RedisPattern[K], RedisPatternEvent[K, V]]
  )

  private object State {
    def fromRefs[F[_]: Concurrent: Log, K, V]: F[State[F, K, V]] =
      (
        SubscriptionMap.makeRef[F, RedisChannel[K], V],
        SubscriptionMap.makeRef[F, RedisPattern[K], RedisPatternEvent[K, V]]
      ).mapN(apply)
  }

  private trait SubscriptionMap[F[_], K, V] {
    def counts: F[Map[K, Long]]

    def subscribe(key: K)(create: Resource[F, Topic[F, V]]): Stream[F, V]

    def unsubscribe(key: K): F[Unit]

    def onMessage(key: K, message: V): F[Unit]
  }

  private object SubscriptionMap {

    private sealed trait SubscriptionState[F[_], V]
    private object SubscriptionState {
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

        override def onMessage(key: K, message: V): F[Unit] =
          ref.get.flatMap(_.get(key).collect { case Active(s) => s }.traverse_(_.topic.publish1(message).void))
      }
  }

}
