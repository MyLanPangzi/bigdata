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
      """
        |SELECT  o.id,
        |        order_status,
        |        user_id,
        |        final_total_amount,
        |        benefit_reduce_amount,
        |        original_total_amount,
        |        feight_fee,
        |        expire_time,
        |        o.create_time,
        |        o.operate_time,
        |        DATE_FORMAT(o.create_time, 'yyyyMMdd') create_date,
        |        DATE_FORMAT(o.create_time, 'HH') create_hour,
        |        true if_first_order,
        |        p.name province_name,
        |        p.area_code province_area_code,
        |        p.iso_code province_iso_code,
        |        u.birthday user_age_group,
        |        u.gender user_gender
        |FROM order_info o
        |LEFT JOIN user_info u on o.user_id = u.id
        |LEFT JOIN base_province p on o.province_id = p.id
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
