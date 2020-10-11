package com.hiscat.flink.scala.source

import org.apache.flink.streaming.api.scala._

object FileSourceWordCount {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    env.readTextFile("E:\\github\\bigdata\\flink\\flink-wordcount\\src\\main\\resources\\word.txt")
      .filter(_.nonEmpty)
      .flatMap(_.split(" "))
      .map((_, 1))
      .keyBy(_._1)
      .sum(1)
      .print()

    env.execute("file source word count")
  }
}
