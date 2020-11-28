package com.hiscat.realtime.analysis

import com.hiscat.flink.sql.parser.DmlCommand
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.types.Row
import org.scalactic.source.Position
import org.scalatest.{BeforeAndAfter, BeforeAndAfterEach}
import org.scalatest.funsuite.AnyFunSuite
import com.hiscat.flink.sql.parser._

class SqlJobSubmitterTest extends AnyFunSuite with BeforeAndAfter with BeforeAndAfterEach {

  var env: StreamExecutionEnvironment = _
  var tEnv: StreamTableEnvironment = _
  before {
    println("before")
    env = StreamExecutionEnvironment.getExecutionEnvironment
    tEnv = StreamTableEnvironment.create(env)
    SqlCommandParser.getCommands(getClass.getResource("/start_event.sql").getPath)
      .filterNot(_.isInstanceOf[DmlCommand])
      .foreach(_.call(tEnv))
  }

  test("kafka read") {
    tEnv.sqlQuery(
      """
        |SELECT *
        |FROM start_event
        |""".stripMargin)
      .toRetractStream[Row]
      .print("test")
  }

  test("uv") {
    val table = tEnv.sqlQuery(
      """
        |SELECT mid, uid, ar, ch, vc, dt, hr, mi, ts
        |FROM (
        |    SELECT
        |    common.mid mid,
        |    common.uid uid ,
        |    common.ar ar,
        |    common.ch ch,
        |    common.vc vc,
        |    FROM_UNIXTIME(ts / 1000, 'yyyy-MM-dd') dt,
        |    FROM_UNIXTIME(ts / 1000, 'HH') hr,
        |    FROM_UNIXTIME(ts / 1000, 'mm') mi,
        |    ts,
        |    ROW_NUMBER() OVER(PARTITION BY common.mid,FROM_UNIXTIME(ts / 1000, 'yyyy-MM-dd') ORDER BY ts) num
        |FROM start_event s
        |)t
        |WHERE t.num = 1
        |""".stripMargin)
    table.executeInsert("dau_index")
    table.toRetractStream[Row]
      .print()
    env.fromElements(1).setParallelism(1).print()
    env.execute("test")
  }
}
