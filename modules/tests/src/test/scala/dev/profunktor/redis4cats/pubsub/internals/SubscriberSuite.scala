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

import cats.effect.IO
import dev.profunktor.redis4cats.data.RedisChannel
import scala.concurrent.duration._

class SubscriberSuite extends IOSuite {

  test("only call redis subscribe and unsubscribe once when streams finish by themselves") {
    val channel = RedisChannel("a")
    for {
      subRef <- IO.ref(0)
      unsubRef <- IO.ref(0)
      map <- make(subRef.update(_ + 1), unsubRef.update(_ + 1))
      a1 <- map.subscribe(channel).take(3).compile.toList.start
      a2 <- map.subscribe(channel).take(2).compile.toList.start
      _ <- IO.sleep(500.millis) // wait for fibers
      _ <- map.onMessage(channel, "one")
      _ <- map.onMessage(channel, "two")
      _ <- map.onMessage(channel, "three")
      _ <- a1.joinWith(notCanceled).map(assertEquals(_, List("one", "two", "three")))
      _ <- a2.joinWith(notCanceled).map(assertEquals(_, List("one", "two")))
      _ <- subRef.get.map(assertEquals(_, 1))
      _ <- unsubRef.get.map(assertEquals(_, 1))
    } yield ()
  }

  test("streams finish on unsubscribe") {
    val channel = RedisChannel("a")
    for {
      unsubRef <- IO.ref(0)
      map <- make(IO.unit, unsubRef.update(_ + 1))
      a1 <- map.subscribe(channel).compile.toList.start
      a2 <- map.subscribe(channel).compile.toList.start
      _ <- IO.sleep(500.millis) // wait for fibers
      _ <- map.onMessage(channel, "one")
      _ <- map.onMessage(channel, "two")
      _ <- map.onMessage(channel, "three")
      _ <- map.unsubscribe(channel)
      _ <- a1.joinWith(notCanceled).map(assertEquals(_, List("one", "two", "three")))
      _ <- a2.joinWith(notCanceled).map(assertEquals(_, List("one", "two", "three")))
      _ <- unsubRef.get.map(assertEquals(_, 1))
    } yield ()
  }

  test("handle unsubscribe failure (Active -> FailedToUnsubscribe -> None)") {
    val channel = RedisChannel("a")
    for {
      unsubRef <- IO.ref(0)
      map <- make(
        IO.unit,
        unsubRef.flatModify {
          case 0 => (1, IO.raiseError[Unit](new RuntimeException("failed")))
          case n => (n + 1, IO.unit)
        }
      )
      a <- map.subscribe(channel).compile.toList.start
      _ <- IO.sleep(500.millis) // wait for fiber
      _ <- map.onMessage(channel, "one")
      _ <- map.unsubscribe(channel)
      _ <- a.join.map(outcome => assert(outcome.isError))
      _ <- map.counts.map(assertEquals(_, Map(channel -> 0L)))
      _ <- map.unsubscribe(channel)
      _ <- map.counts.map(assertEquals(_, Map.empty[RedisChannel[String], Long]))
    } yield ()
  }

  test("handle subscribe failure (None -> Subscribing -> None)") {
    val channel = RedisChannel("a")
    for {
      unsubRef <- IO.ref(0)
      map <- make(IO.raiseError(new RuntimeException("fail subscribe")), unsubRef.update(_ + 1))
      a <- map.subscribe(channel).compile.toList.start
      _ <- IO.sleep(500.millis) // wait for fiber
      _ <- a.join.map(outcome => assert(outcome.isError))
      _ <- map.counts.map(assertEquals(_, Map.empty[RedisChannel[String], Long]))
      _ <- unsubRef.get.map(assertEquals(_, 0))
    } yield ()
  }

  private def make(sub: IO[Unit], unsub: IO[Unit]): IO[Subscriber.SubscriptionMap[IO, RedisChannel[String], String]] = {
    // import effect.Log.Stdout._
    import effect.Log.NoOp._
    Subscriber.SubscriptionMap.singleRef[IO, RedisChannel[String], String](
      new Subscriber.SubscriptionCommands[IO, RedisChannel[String]]  {
        override def subscribe(key: RedisChannel[String]): IO[Unit] = sub
        override def unsubscribe(key: RedisChannel[String]): IO[Unit] = unsub
      }
    )
  }

  private def notCanceled[A]: IO[A] = IO.raiseError(new RuntimeException("should not be canceled"))

}
