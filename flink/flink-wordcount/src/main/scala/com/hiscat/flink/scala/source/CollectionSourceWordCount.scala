package com.hiscat.flink.scala.source

import org.apache.flink.streaming.api.scala._

object CollectionSourceWordCount {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    env.fromCollection(
      List(
        "hello world",
        "hello flink",
        "hello scala"
      )
    )
      .filter(_.nonEmpty)
      .flatMap(_.split(" "))
      .map((_, 1))
      .keyBy(_._1)
      .sum(1)
      .print()

    env.execute("collection source word count")
  }
}
