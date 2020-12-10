package com.hiscat.flink.connector.phoenix

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.api._
import org.apache.flink.types.Row

object MainTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val tEnv = StreamTableEnvironment.create(env)

    registerTableT(env, tEnv)
    registerTestTable(tEnv)
//    testUpsert(env, tEnv)
    testLookup(tEnv)
    env.execute("test")
  }

  private def registerTestTable(tEnv: StreamTableEnvironment) = {
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
  }

  private def registerTableT(env: StreamExecutionEnvironment, tEnv: StreamTableEnvironment): Unit = {
    val input = env.fromElements(
      (1, "hello")
    )
    tEnv.createTemporaryView("t", input, $"id", $"name", $"proctime".proctime)
  }

  private def testUpsert(env: StreamExecutionEnvironment, tEnv: StreamTableEnvironment) = {
    tEnv.executeSql(
      """
        |INSERT INTO test
        |SELECT id,name
        |FROM t
        |""".stripMargin)
  }

  private def testLookup(tEnv: StreamTableEnvironment) = {
    tEnv.sqlQuery(
      """
        |SELECT t.id,test.name
        |FROM t
        |LEFT JOIN test FOR SYSTEM_TIME AS OF t.proctime AS test ON test.id = t.id
        |""".stripMargin)
      .toRetractStream[Row]
      .print()
  }
}
