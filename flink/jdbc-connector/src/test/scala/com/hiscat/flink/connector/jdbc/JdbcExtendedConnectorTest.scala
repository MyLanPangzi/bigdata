package com.hiscat.flink.connector.jdbc

import java.sql.Connection
import java.util.Properties

import com.hiscat.flink.sql.parser.SqlCommandParser
import com.mysql.jdbc.Driver
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.api._
import org.apache.flink.types.Row
import org.apache.phoenix.jdbc.PhoenixDriver
import org.scalatest.{BeforeAndAfter, BeforeAndAfterEach}
import org.scalatest.funsuite.AnyFunSuite

class JdbcExtendedConnectorTest extends AnyFunSuite with BeforeAndAfter with BeforeAndAfterEach {

  var env: StreamExecutionEnvironment = _
  var tEnv: StreamTableEnvironment = _
  before {
    env = StreamExecutionEnvironment.getExecutionEnvironment
    tEnv = StreamTableEnvironment.create(env)
    env.setParallelism(1)
  }
  test("write") {
    SqlCommandParser.getCommands(getClass.getResource("/phoenix.sql").getPath)
      .foreach(_.call(tEnv))

    try {
      val input = env.fromElements((1, false)).setParallelism(1)
      val table = tEnv.fromDataStream(input, $"id", $"status")
      //      table.printSchema()
      table
        .toAppendStream[Row]
        .print("test")
      table.executeInsert("first_order_user_status")
      env.execute("test")
    } catch {
      case e: Throwable => println(e)
    }
  }

  test("phoenix driver") {
    val driver = new PhoenixDriver()
    val connection = driver.connect("jdbc:phoenix:hadoop102", new Properties())
    println(connection)
  }
  test("mysql driver") {
    val driver = new Driver
    val info = new Properties
    info.setProperty("user", "root")
    val connection = driver.connect("jdbc:mysql://hadoop102:3306", info)
    System.out.println(connection)
  }

  override protected def afterEach(): Unit = {

    //    env.execute("test")
  }
}
