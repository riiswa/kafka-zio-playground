package com.warisradji.kzplayground.simple

import com.warisradji.kzplayground.zio.KafkaConsumerZIO
import org.apache.kafka.clients.consumer.{ConsumerRecord, KafkaConsumer}
import zio.*
import zio.stream.ZStream

import java.util.Properties
import scala.jdk.CollectionConverters.*

object SimpleConsumerZIO extends App {
  val props = new Properties() {
    this.putAll(Map(
      "bootstrap.servers" -> "localhost:29092,localhost:39092",
      "group.id" -> "test",
      "enable.auto.commit" -> true,
      "auto.commit.interval.ms" -> 1000,
      "session.timout.ms" -> 30000,
      "key.deserializer" -> "org.apache.kafka.common.serialization.IntegerDeserializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
    ).asJava)
  }

  def receiveMessages(consumer: KafkaConsumer[Int, String]): ZIO[Has[Console], Throwable, Unit] =
    consumer.pollZIO(100).foldZIO(
      err => Console.printError(s"Failed with: $err") *> ZIO.fail(err),
      (zs: ZStream[Any, Throwable, ConsumerRecord[Int, String]]) => zs.tap(
        record => Console.printLine(s"offset = ${record.offset()}, key = ${record.key()}, value = ${record.value()}").orDie *> ZIO.succeed(record)
      ).drain.runDrain.onError(
        err => Console.printError(s"Failed with: $err").orDie
      )
    ) *> receiveMessages(consumer)

  def run(args: List[String]): URIO[Has[Console], ExitCode] = (for {
    topicName <- args.headOption match {
      case Some(tn) => ZIO.succeed(tn)
      case None => Console.printError("Argument topicName is required.") *> ZIO.fail(None)
    }
    consumer <- KafkaConsumerZIO.create[Int, String](props).onError(
      err => Console.printError(s"Failed with: $err").orDie
    )
    _ <- consumer.subscribeZIO(topicName :: Nil)
    _ <- receiveMessages(consumer)
  } yield ()).exitCode

}
