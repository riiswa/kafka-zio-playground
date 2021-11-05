package com.warisradji.kzplayground.zio

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.common.errors.InterruptException
import zio.*

import java.util.Properties
import scala.concurrent.Future

object KafkaPoducerZIO {
  def create[K, V](props: Properties): IO[ConfigException, KafkaProducer[K, V]] =
    ZIO.succeed(new KafkaProducer(props))
}

implicit class KafkaPoducerZIO[K, V](producer: KafkaProducer[K, V]) {
  def sendZIO(topicName: String, key: K, value: V): Task[RecordMetadata] = ZIO.fromFuture {
      implicit ec =>
        Future {
          producer.send(new ProducerRecord[K, V](topicName, key, value)).get
        }
    }

    def closeZIO(): IO[InterruptException, Unit] = ZIO.succeed(producer.close())
}
