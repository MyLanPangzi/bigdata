package com.hiscat.flink.scala.sink

import com.hiscat.flink.scala.WordCount
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer

object KafkaSink {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.fromElements(
      WordCount("hello", 1, 1),
      WordCount("world", 1, 1),
    )
      .map(_.toString)
      .addSink(
        new FlinkKafkaProducer[String](
          "hadoop102:9092",
          "flink_kafka_sink", // target topic
          new SimpleStringSchema() // serialization schema
        )
      )

    env.execute("kafka sink")
  }
}
