package com.hiscat.flink.scala.source

import com.hiscat.flink.scala.WordSource
import org.apache.flink.streaming.api.scala._

object CustomSourceWordCount {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    env.addSource(new WordSource)
      .map(_.line)
      .filter(_.nonEmpty)
      .flatMap(_.split(" "))
      .map((_, 1))
      .keyBy(_._1)
      .sum(1)

      .print()

    env.execute("custom source word count")

  }
}
