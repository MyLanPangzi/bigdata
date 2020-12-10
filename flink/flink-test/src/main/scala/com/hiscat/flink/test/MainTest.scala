package com.hiscat.flink.test

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.api._
import org.apache.flink.types.Row

object MainTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironment.create(env)

    val input = env.fromElements(1)
    val table = tEnv.fromDataStream(input, $"id")
    table.toAppendStream[Row].print()

    env.execute("test")
  }
}
