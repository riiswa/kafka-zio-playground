package com.warisradji.kzplayground.simple

import com.warisradji.kzplayground.zio.KafkaPoducerZIO
import org.apache.kafka.clients.producer.KafkaProducer
import zio.*
import zio.stream.ZStream

import java.util.Properties
import scala.jdk.CollectionConverters.*

object SimpleProducerZIO extends App {
  val props = new Properties() {
    this.putAll(Map(
      "bootstrap.servers" -> "localhost:29092,localhost:39092",
      "acks" -> "all",
      "retries" -> 0,
      "batch.size" -> 16384,
      "linger.ms" -> 1,
      "buffer.memory" -> 33554432,
      "key.serializer" -> "org.apache.kafka.common.serialization.IntegerSerializer",
      "value.serializer" -> "org.apache.kafka.common.serialization.StringSerializer",
    ).asJava)
  }

  def sendMessages(producer: KafkaProducer[Int, String], topicName: String): ZIO[Has[Clock], Throwable, Unit] =
    ZStream.range(1, 11).schedule(Schedule.fixed(2.seconds)).tap(
      i => producer.sendZIO(topicName, i, i.toString)
    ).drain.runDrain

  def run(args: List[String]): URIO[Has[Clock] & Has[Console], ExitCode] = (for {
    topicName <- args.headOption match {
      case Some(tn) => ZIO.succeed(tn)
      case None => Console.printError("Argument topicName is required.") *> ZIO.fail(None)
    }
    producer <- KafkaPoducerZIO.create[Int, String](props).onError(
      err => Console.printError(s"Failed with: $err").orDie
    )
    _ <- sendMessages(producer, topicName).foldZIO(
      err => Console.printError(s"Failed with: $err"),
      _ => Console.printLine("Messages sent successfully.")
    ) *> producer.closeZIO()
  } yield ()).exitCode
}
