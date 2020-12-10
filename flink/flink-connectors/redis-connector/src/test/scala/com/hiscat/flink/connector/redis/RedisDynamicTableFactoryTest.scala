package com.hiscat.flink.connector.redis

import com.hiscat.flink.sql.parser.SqlCommandParser
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.api._
import org.apache.flink.types.Row
import org.scalatest.{BeforeAndAfter, BeforeAndAfterEach}
import org.scalatest.funsuite.AnyFunSuite

class RedisDynamicTableFactoryTest extends AnyFunSuite with BeforeAndAfterEach with BeforeAndAfter {

  var env: StreamExecutionEnvironment = _
  var tEnv: StreamTableEnvironment = _
  before {
    env = StreamExecutionEnvironment.getExecutionEnvironment
    tEnv = StreamTableEnvironment.create(env)
  }

  test("write") {
    SqlCommandParser.getCommands(getClass.getResource("/redis.sql").getPath)
      .foreach(_.call(tEnv))

    val input = env.fromElements((1, false))
    val table = tEnv.fromDataStream(input, $"id", $"status")
    table.printSchema()
    table
      .toAppendStream[Row]
      .print("test")
    table
      .executeInsert("redis")
  }

  test("read") {
    SqlCommandParser.getCommands(getClass.getResource("/redis.sql").getPath)
      .foreach(_.call(tEnv))

    val input = env.fromElements((1, "hello"))
    tEnv.createTemporaryView("u", input, $"id", $"name", $"proctime".proctime)
    val table = tEnv.sqlQuery(
      """
        |SELECT u.id,u.name,r.status
        |FROM u
        |LEFT JOIN redis FOR SYSTEM_TIME AS OF u.proctime AS r ON r.id = u.id
        |""".stripMargin)
    table.printSchema()
    table
      .toAppendStream[Row]
      .print("test")
  }

  override protected def afterEach(): Unit = {
//    env.fromElements(1).setParallelism(1).print()
    env.execute("test")
  }
}
