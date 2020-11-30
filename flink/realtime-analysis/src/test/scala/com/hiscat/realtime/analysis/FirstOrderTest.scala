package com.hiscat.realtime.analysis

import com.hiscat.flink.sql.parser.{DmlCommand, _}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.types.Row
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.{BeforeAndAfter, BeforeAndAfterEach}

class FirstOrderTest extends AnyFunSuite with BeforeAndAfter with BeforeAndAfterEach {

  var env: StreamExecutionEnvironment = _
  var tEnv: StreamTableEnvironment = _
  before {
    env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    tEnv = StreamTableEnvironment.create(env)
    SqlCommandParser.getCommands(getClass.getResource("/first_order.sql").getPath)
      .filterNot(_.isInstanceOf[DmlCommand])
      .foreach(e => {
        //        println(e)
        e.call(tEnv)
      })
  }

  test("read order_info") {
    val table = tEnv.sqlQuery(
      """
        |SELECT *
        |FROM order_info
        |""".stripMargin)
    table.printSchema()
    table
      .toRetractStream[Row]
      .print("test")
  }

  test("read user_info") {
    val table = tEnv.sqlQuery(
      """
        |SELECT *
        |FROM user_info
        |""".stripMargin)
    table.printSchema()
    table
      .toRetractStream[Row]
      .print("test")
  }

  test("first order") {
    val table = tEnv.sqlQuery(
      """SELECT *
        |FROM first_order_view
        |""".stripMargin)
    table.printSchema()
    table.toRetractStream[Row]
      .print()
    table.executeInsert("first_order_index")
  }


  override protected def afterEach(): Unit = {
    env.execute("test")
  }
}
