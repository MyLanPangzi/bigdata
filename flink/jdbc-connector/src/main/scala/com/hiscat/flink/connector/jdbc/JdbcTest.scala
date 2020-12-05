package com.hiscat.flink.connector.jdbc

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.api._
import org.apache.flink.types.Row

object JdbcTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironment.create(env)
    env.setParallelism(1)
    val input = env.fromElements((1, false)).setParallelism(1)
    val table = tEnv.fromDataStream(input, $"id", $"status")
    tEnv.executeSql(
      """
        |CREATE TABLE first_order_user_status (
        |  id BIGINT,
        |  status BOOLEAN,
        |  PRIMARY KEY (id) NOT ENFORCED
        |) WITH (
        |   'connector' = 'jdbc-extended',
        |   'url' = 'jdbc:phoenix:hadoop102',
        |   'driver' = 'org.apache.phoenix.jdbc.PhoenixDriver',
        |   'table-name' = 'USER_ORDER_STATUS',
        |   'enable-upsert' = 'true'
        |)
        |""".stripMargin)
    table
      .toAppendStream[Row]
      .print("test")
    table.executeInsert("first_order_user_status")
    env.execute("test")
  }
}
