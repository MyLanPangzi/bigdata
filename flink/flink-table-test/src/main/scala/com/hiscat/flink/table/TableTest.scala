package com.hiscat.flink.table

import org.apache.flink.streaming.api._
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.types.Row

object TableTest {

  case class Event(uid: String, behavior: String, ts: Long)

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tableEnv = StreamTableEnvironment.create(env)

    val input = env.fromElements(
      Event("hello", "click", 1000),
      Event("hello", "click", 3000),
      Event("world", "click", 5000),
      Event("world", "click", 9000)
    )
      .assignAscendingTimestamps(_.ts)

    //    input.print("data stream")

    val table = tableEnv.fromDataStream(input, $"uid", $"behavior", $"ts".rowtime)
    //    table.toAppendStream[Row].print("table api")

    table.window(Tumble over 5.second on $"ts" as $"w")
      .groupBy($"w", $"uid")
      .select($"w".`end`, $"uid", $"uid".count)
      .toAppendStream[Row]
      .print("agg table")

    tableEnv.createTemporaryView("event", input, $"uid", $"behavior", $"ts".rowtime)
    tableEnv.sqlQuery(
      """
        |SELECT TUMBLE_END(ts, INTERVAL '10' SECOND), uid, count(*) cnt
        |FROM event
        |GROUP BY uid, TUMBLE(ts, INTERVAL '10' SECOND)
        |""".stripMargin
    )
      .toAppendStream[Row]
      .print("sql")

    env.execute("table api test")
  }
}
