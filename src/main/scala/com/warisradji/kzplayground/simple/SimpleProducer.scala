package com.warisradji.kzplayground.simple

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import java.util.Properties
import scala.jdk.CollectionConverters.*

object SimpleProducer {
  @main def simpleProducerMain(topicName: String): Unit = {
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

    new KafkaProducer[Int, String](props) {
      (1 to 10)
        .foreach(i =>
          this.send(new ProducerRecord[Int, String](topicName, i, i.toString))
        )
      this.close()
    }

    println("Messages sent successfully.")
  }

}
