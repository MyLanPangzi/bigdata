package com.hiscat.flink.scala.transformation

import org.apache.flink.streaming.api.scala._

object Union {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    env.fromElements("hello world")
      .union(env.fromElements("hello flink"))
      .flatMap(_.split(" "))
      .filter(_.nonEmpty)
      .map((_, 1))
      .keyBy(_._1)
      .sum(1)
      .print()

    env.execute("union transformation")
  }
}
