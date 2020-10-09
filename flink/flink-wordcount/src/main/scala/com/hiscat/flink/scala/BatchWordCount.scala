package com.hiscat.flink.scala

import org.apache.flink.api.scala._

object BatchWordCount {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

    env.readTextFile("E:\\github\\bigdata\\flink\\flink-wordcount\\src\\main\\resources\\word.txt")
      .filter(!_.isEmpty)
      .flatMap(_.split(" "))
      .map((_, 1))
      .groupBy(0)
      .sum(1)
      .print()

//    env.execute("batch word count")
  }
}
