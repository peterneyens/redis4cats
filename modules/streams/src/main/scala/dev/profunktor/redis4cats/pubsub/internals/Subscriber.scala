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

import cats.{ Applicative, ApplicativeThrow }
import cats.effect.kernel._
import cats.effect.std.{ Dispatcher, MapRef }
import cats.syntax.all._
import dev.profunktor.redis4cats.data.{ RedisChannel, RedisPattern, RedisPatternEvent }
import dev.profunktor.redis4cats.effect.{ FutureLift, Log }
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
      // We only use a single listener for all channels and patterns.
      // Since we have a map of all subscriptions, we can dispatch messages to
      // the right topic directly.
      // Lettuce calls the listeners one by one (for every subscribe,
      // unsubscribe, message, ...), so using multiple listeners when we can
      // find the right subscription easily, is inefficient.
      _ <- Resource.make {
             val listener = State.listener(state, dispatcher)
             Sync[F].delay(subConnection.addListener(listener)).as(listener)
           }(listener => Sync[F].delay(subConnection.removeListener(listener)))
    } yield new Subscriber(state, subConnection)

  private def subscribe[F[_]: Async: Log, TypedKey, SubValue, K, V](
      key: TypedKey,
      state: SubscriptionMap[F, TypedKey, SubValue],
      subscribeToRedis: F[Unit],
      unsubscribeFromRedis: F[Unit]
  ): Stream[F, SubValue] =
    state.subscribe(key) {
      Resource.eval(Log[F].info(s"Creating subscription for $key")) >>
        Resource.make(
          subscribeToRedis
        )(_ => unsubscribeFromRedis <* Log[F].debug(s"Unsubscribed from $key")) >>
        Resource.eval(Log[F].debug(s"Created subscription for $key"))
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

    def listener[F[_], K, V](
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
  }

  private trait SubscriptionMap[F[_], K, V] {
    def counts: F[Map[K, Long]]

    def subscribe(key: K)(redisSubscribe: Resource[F, Unit]): Stream[F, V]

    def unsubscribe(key: K): F[Unit]

    def onMessage(key: K, message: V): F[Unit]
  }

  private object SubscriptionMap {

    private sealed trait SubscriptionState[F[_], V]
    private object SubscriptionState {
      final case class Active[F[_], V](subscription: Redis4CatsSubscription[F, V]) extends SubscriptionState[F, V]
      final case class Subscribing[F[_], V](done: F[Unit]) extends SubscriptionState[F, V]
      final case class Unsubscribing[F[_], V](done: F[Unit], unsubscribe: F[Unit]) extends SubscriptionState[F, V]
      // currently this leaves a Redis4CatsSubscription with a single subscriber
      // (even when there is no subscriber anymore)
      // https://github.com/profunktor/redis4cats/blob/7924e89913c75b8675a865a822c51fc97405be17/modules/streams/src/main/scala/dev/profunktor/redis4cats/pubsub/internals/Subscriber.scala#L94
      final case class FailedToUnsubscribe[F[_], V](unsubscribe: F[Unit]) extends SubscriptionState[F, V]
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

        override def subscribe(key: K)(redisSubscribe: Resource[F, Unit]): Stream[F, V] =
          Stream.eval(addSubscription(key)(redisSubscribe)).flatMap(_.stream(remove(key)))

        private def addSubscription(key: K)(redisSubscribe: Resource[F, Unit]): F[Redis4CatsSubscription[F, V]] =
          Deferred[F, Unit].flatMap { d =>
            // returning an `F[Redis4CatsSubscription[F, V]]]` so we can wait on
            // subcribing/unsubscribing to end outside of cancelation
            ref
              .flatModify[F[Redis4CatsSubscription[F, V]]] { subscribers =>
                subscribers.get(key) match {
                  case Some(Active(subscription)) =>
                    // We have an existing subscription, mark that it has one more subscriber.
                    val newSubscription = subscription.addSubscriber
                    val log = Log[F].debug(
                      s"Returning existing subscription for $key, " +
                        s"subscribers: ${subscription.subscribers} -> ${newSubscription.subscribers}"
                    )
                    (subscribers.updated(key, Active(newSubscription)), log.as(newSubscription).pure[F])
                  case Some(Unsubscribing(wait, _)) =>
                    // an existing subscription is getting shut down, wait and try again
                    (subscribers, (wait >> addSubscription(key)(redisSubscribe)).pure[F])
                  case Some(Subscribing(wait)) =>
                    // an existing subscription is getting created, wait and try again
                    (subscribers, (wait >> addSubscription(key)(redisSubscribe)).pure[F])
                  case Some(FailedToUnsubscribe(unsubscribe)) =>
                    // an existing subscription that we failed to unsubscribe,
                    // no need to subscribe, we only need a new topic
                    val action = Topic[F, V].flatMap { topic =>
                      val subscription = Redis4CatsSubscription(topic, subscribers = 1, unsubscribe)
                      mapRef(key).flatModify[F[Redis4CatsSubscription[F, V]]] {
                        case Some(Subscribing(_)) =>
                          (Some(Active(subscription)), d.complete(()).as(subscription.pure[F]))
                        case other => (other, ApplicativeThrow[F].raiseError(new RuntimeException)) // TODO
                      }
                    }
                    (subscribers.updated(key, Subscribing(d.get)), action)
                  case None =>
                    // No existing subscription, create a new one.
                    val keyRef   = mapRef(key)
                    val complete = d.complete(()).void
                    val start = redisSubscribe
                      .evalMap(_ => Topic[F, V])
                      .allocated
                      .onError { case _ =>
                        keyRef.flatModify {
                          case Some(Subscribing(_)) => (None, complete)
                          case _                    => (None, complete) // TODO
                        }
                      }
                      .flatMap { case (topic, cleanup) =>
                        val subscription = Redis4CatsSubscription(topic, subscribers = 1, cleanup)
                        keyRef.flatModify {
                          case Some(Subscribing(_)) => (Some(Active(subscription)), complete.as(subscription))
                          case _                    =>
                            // this would be a bug, we only expect a starting subscription
                            val action = cleanup.handleError(_ => ()) >> // TODO ?
                              complete >>
                              Log[F].error(
                                s"Subscription in unexpected state after creation, this is a bug in redis4cats!"
                              ) >>
                              ApplicativeThrow[F].raiseError[Redis4CatsSubscription[F, V]](
                                new IllegalStateException(
                                  "Subscription could not be created after invalid state change"
                                )
                              )
                            (None, action)
                        }
                      }
                    val logged = Log[F].info(s"Creating subscription for $key") *> start <* Log[F].debug(
                      s"Created subscription for $key"
                    )
                    (subscribers.updated(key, Subscribing(d.get)), logged.map(_.pure[F]))
                }
              }
              .flatten
          }

        private def remove(key: K): F[Unit] =
          Deferred[F, Unit].flatMap { d =>
            val keyRef = mapRef(key)
            keyRef.flatModify {
              case Some(Active(sub)) =>
                if (sub.isLastSubscriber) {
                  val complete = d.complete(()).void
                  val cleanup = sub.cleanup
                    .onError { case _ =>
                      keyRef.flatModify {
                        case Some(Unsubscribing(_, unsub)) => (Some(FailedToUnsubscribe(unsub)), complete)
                        case other                         => (other, complete) // TODO
                      }
                    }
                    .>>(
                      keyRef.flatModify {
                        case Some(Unsubscribing(_, _)) => (None, complete)
                        case other =>
                          val action =
                            complete >>
                              Log[F].error(
                                "Subscription in unexpected state after cleanup, this is a bug in redis4cats!"
                              )
                          (other, action)
                      }
                    )
                  (Some(Unsubscribing(d.get, cleanup)), cleanup)
                } else (Some(Active(sub.removeSubscriber)), Applicative[F].unit)
              case other =>
                // `remove` is only called from `subscribe` after we have an active subscription,
                // so we shouldn't get a `remove` for `None` or `Subscribing`.
                // We can only end up in `Unsubscribing` after the last `remove` for a subscription
                // so we shouldn't get a `remove` for `Unsubscribing`.
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
            case Some(Unsubscribing(_, _)) => Applicative[F].unit
            // `close` will terminate all streams, which will perform cleanup
            // once the last stream terminates.
            case Some(Active(sub)) =>
              // should we unsubscribe here already?
              // `Topic#publish` after closing is a no op, any new messages
              // won't be observed
              Log[F].info(s"Unsubscribing from $key with ${sub.subscribers} subscribers") >>
                sub.topic.close.void
            // wait until the subscription has started and unsubscribe
            case Some(Subscribing(wait)) => wait >> unsubscribe(key)
            //
            case Some(FailedToUnsubscribe(unsubscribe)) => unsubscribe // TODO remove from map
          }

        override def onMessage(key: K, message: V): F[Unit] =
          ref.get.flatMap(
            _.get(key) match {
              case Some(Active(s)) =>
                // this will block the lettuce netty handler if `publish1`
                // symantically blocks when one of the topic subscribers is
                // behind
                s.topic.publish1(message).void
              case Some(Subscribing(wait)) =>
                // this will block the lettuce netty handler
                wait >> onMessage(key, message)
              case Some(Unsubscribing(_, _))    => Applicative[F].unit
              case Some(FailedToUnsubscribe(_)) => Applicative[F].unit
              case None                         =>
                // We expect that all SUBSCRIBE commands happen through
                // `subscribe`. so we should never receive message without
                // subscriptions
                Log[F].info(s"Received message for $key without subscription")
            }
          )
      }
  }

}
