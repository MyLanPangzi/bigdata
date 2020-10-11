package com.hiscat.flink.scala.transformation.co

import org.apache.flink.streaming.api.scala._

object Connect {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val s1 = env.fromElements("hello world")
    val s2 = env.fromElements("hello flink")
    s1.connect(s2)
      .flatMap(_.split(" "), _.split(" "))
      .print()

    env.execute("connect word count")
  }
}
