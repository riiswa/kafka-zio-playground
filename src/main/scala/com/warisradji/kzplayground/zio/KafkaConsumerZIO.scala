package com.warisradji.kzplayground.zio

import org.apache.kafka.clients.consumer.{ConsumerRecord, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.common.config.ConfigException
import zio.*
import zio.stream.ZStream

import java.util.Properties
import scala.jdk.CollectionConverters.*

object KafkaConsumerZIO {
  def create[K, V](props: Properties):  IO[ConfigException, KafkaConsumer[K, V]] =
    ZIO.succeed(new KafkaConsumer(props))
}

implicit class KafkaConsumerZIO[K, V](consumer: KafkaConsumer[K, V]) {
  def subscribeZIO(topics: List[String]): Task[Unit] =
    ZIO.attempt(consumer.subscribe(topics.asJava))

  def pollZIO(ms: Long): Task[ZStream[Any, Throwable, ConsumerRecord[K, V]]] =
    ZIO.attempt(ZStream.fromIterator(consumer.poll(java.time.Duration.ofMillis(ms)).iterator().asScala))
}
