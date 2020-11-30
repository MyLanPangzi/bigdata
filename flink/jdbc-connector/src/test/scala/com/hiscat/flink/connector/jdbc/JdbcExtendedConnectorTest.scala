package com.hiscat.flink.connector.jdbc

import com.hiscat.flink.sql.parser.SqlCommandParser
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.api._
import org.apache.flink.types.Row
import org.scalatest.{BeforeAndAfter, BeforeAndAfterEach}
import org.scalatest.funsuite.AnyFunSuite

class JdbcExtendedConnectorTest extends AnyFunSuite with BeforeAndAfter with BeforeAndAfterEach {

  var env: StreamExecutionEnvironment = _
  var tEnv: StreamTableEnvironment = _
  before {
    env = StreamExecutionEnvironment.getExecutionEnvironment
    tEnv = StreamTableEnvironment.create(env)
  }
  test("write") {
    SqlCommandParser.getCommands(getClass.getResource("/phoenix.sql").getPath)
      .foreach(_.call(tEnv))

    val input = env.fromElements((1, false))
    val table = tEnv.fromDataStream(input, $"id", $"status")
    table.printSchema()
    table
      .toAppendStream[Row]
      .print("test")
    table
      .executeInsert("first_order_user_status")
    env.execute("test")
  }

  override protected def afterEach(): Unit = {

    //    env.execute("test")
  }
}
