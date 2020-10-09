package com.hiscat.flink.scala

import org.apache.flink.streaming.api.scala._

object StreamWordCount {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

//    env.setParallelism(1)

    env.fromElements("hello world", "hello flink", "hello scala", "hello hadoop")
      .filter(_.nonEmpty)
      .flatMap(_.split(" "))
      .map((_, 1))
      .keyBy(_._1)
      .sum(1)
      .print().setParallelism(1)

    env.execute("stream word count")
  }
}
