package com.warisradji.kzplayground.simple

import org.apache.kafka.clients.consumer.KafkaConsumer

import java.util.Properties
import scala.jdk.CollectionConverters.*

object SimpleConsumer {
  @main def simpleConsumerMain(topicName: String): Unit = {
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

    val consumer = new KafkaConsumer[Int, String](props)
    consumer.subscribe(List(topicName).asJava)
    println(s"Subscribed to topic $topicName")

    @annotation.tailrec
    def run(i: Int = 0): Int = {
      val records = consumer.poll(java.time.Duration.ofMillis(100))
      records.forEach(
        record => println(s"offset = ${record.offset()}, key = ${record.key()}, value = ${record.value()}")
      )
      run(i + 1)
    }

    run(0)

  }
}
