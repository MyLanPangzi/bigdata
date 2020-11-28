package com.hiscat.realtime.analysis

import com.hiscat.flink.sql.parser.{DmlCommand, _}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.types.Row
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.{BeforeAndAfter, BeforeAndAfterEach}

class DimTest extends AnyFunSuite with BeforeAndAfter with BeforeAndAfterEach {

  var env: StreamExecutionEnvironment = _
  var tEnv: StreamTableEnvironment = _
  before {
    env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    tEnv = StreamTableEnvironment.create(env)
    SqlCommandParser.getCommands(getClass.getResource("/dim.sql").getPath)
      .filterNot(_.isInstanceOf[DmlCommand])
      .foreach(e => {
//        println(e)
        e.call(tEnv)
      })
  }

  test("read base_province") {
    val table = tEnv.sqlQuery(
      """
        |SELECT *
        |FROM base_province
        |""".stripMargin)
    table.printSchema()
    table
      .toRetractStream[Row]
      .print("test")
  }

  test("read base_region") {
    val table = tEnv.sqlQuery(
      """
        |SELECT *
        |FROM base_region
        |""".stripMargin)
    table.printSchema()
    table
      .toRetractStream[Row]
      .print("test")
  }

  test("read base_trademark") {
    val table = tEnv.sqlQuery(
      """
        |SELECT *
        |FROM base_trademark
        |""".stripMargin)
    table.printSchema()
    table
      .toRetractStream[Row]
      .print("test")
  }

  test("read base_category1") {
    val table = tEnv.sqlQuery(
      """
        |SELECT *
        |FROM base_category1
        |""".stripMargin)
    table.printSchema()
    table
      .toRetractStream[Row]
      .print("test")
  }
  test("read base_category2") {
    val table = tEnv.sqlQuery(
      """
        |SELECT *
        |FROM base_category2
        |""".stripMargin)
    table.printSchema()
    table
      .toRetractStream[Row]
      .print("test")
  }

  test("read base_category3") {
    val table = tEnv.sqlQuery(
      """
        |SELECT *
        |FROM base_category3
        |""".stripMargin)
    table.printSchema()
    table
      .toRetractStream[Row]
      .print("test")
  }
  test("read sku_info") {
    val table = tEnv.sqlQuery(
      """
        |SELECT *
        |FROM sku_info
        |""".stripMargin)
    table.printSchema()
    table
      .toRetractStream[Row]
      .print("test")
  }
  test("read spu_info") {
    val table = tEnv.sqlQuery(
      """
        |SELECT *
        |FROM spu_info
        |""".stripMargin)
    table.printSchema()
    table
      .toRetractStream[Row]
      .print("test")
  }

  override protected def afterEach(): Unit = {
    env.execute("test")
  }
}
