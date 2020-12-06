package com.hiscat.flink.connector.phoenix

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.api._

object MainTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val tEnv = StreamTableEnvironment.create(env)

    tEnv.executeSql(
      """
        |CREATE TABLE test(
        | id INT,
        | name STRING,
        | PRIMARY KEY(id) NOT ENFORCED
        |)WITH(
        | 'connector' = 'phoenix',
        | 'url' = 'jdbc:phoenix:hadoop102',
        | 'table-name' = 'test'
        |)
        |""".stripMargin)
    val input = env.fromElements(
      (1, "hello"),
      (1, "world")
    )
    tEnv.createTemporaryView("t", input, $"id", $"name")
    tEnv.executeSql(
      """
        |INSERT INTO test
        |SELECT *
        |FROM t
        |""".stripMargin)
    env.fromElements(1).print()
    env.execute("test")
  }
}
