package com.hiscat.market.analysis

import java.time.{LocalDateTime, ZoneOffset}

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.types.Row
import org.apache.flink.util.Collector

object MarketingUserBehaviorAnalysis {

  case class MarketingCountView(var windowStart: LocalDateTime,
                                var windowEnd: LocalDateTime,
                                var channel: String,
                                var behavior: String,
                                var count: Long)

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tableEnv = StreamTableEnvironment.create(env)

    val input = env.addSource(new SimulateEventSource)
      .assignAscendingTimestamps(_.ts)
      .filter(_.behavior != "UNINSTALL")

    sql(tableEnv, input)
    //    tableApi(tableEnv, input)
    //    dataStreamApi(input)

    env.execute("marketing user behavior analysis")
  }

  private def sql(tableEnv: StreamTableEnvironment, input: DataStream[MarketingUserBehavior]) = {
    tableEnv.createTemporaryView("event", input, $"ts".rowtime, $"channel", $"behavior")
    tableEnv.sqlQuery(
      """
        |SELECT HOP_START(ts, INTERVAL '1' SECOND, INTERVAL '1' HOUR),
        |       HOP_END(ts, INTERVAL '1' SECOND, INTERVAL '1' HOUR),
        |       channel,
        |       behavior,
        |       count(*)
        |FROM event
        |GROUP BY HOP(ts, INTERVAL '1' SECOND, INTERVAL '1' HOUR), channel, behavior
        |""".stripMargin)
      .toAppendStream[Row]
      .print("sql")
  }

  private def tableApi(tableEnv: StreamTableEnvironment, input: DataStream[MarketingUserBehavior]) = {
    tableEnv.fromDataStream(input, $"channel", $"behavior", $"ts".rowtime)
      .window(Slide over 1.hour every 1.second on $"ts" as $"w")
      .groupBy($"w", $"channel", $"behavior")
      .select($"w".start, $"w".end, $"channel", $"behavior", $"behavior".count)
      .toAppendStream[Row]
      .print("table")
  }

  private def dataStreamApi(input: DataStream[MarketingUserBehavior]) = {
    input.keyBy(e => (e.channel, e.behavior))
      .timeWindow(Time.hours(1), Time.seconds(1))
      .aggregate(
        new MarketingCountAgg,
        (_: (String, String), w: TimeWindow, v: Iterable[MarketingCountView], out: Collector[MarketingCountView]) => {
          v.head.windowStart = LocalDateTime.ofEpochSecond(w.getStart / 1000, 0, ZoneOffset.ofHours(8))
          v.head.windowEnd = LocalDateTime.ofEpochSecond(w.getEnd / 1000, 0, ZoneOffset.ofHours(8))
          out.collect(v.head)
        }
      )
      .print("data stream")
  }

  class MarketingCountAgg extends AggregateFunction[MarketingUserBehavior, MarketingCountView, MarketingCountView] {
    override def createAccumulator(): MarketingCountView = MarketingCountView(null, null, "", "", 0)

    override def add(value: MarketingUserBehavior, accumulator: MarketingCountView): MarketingCountView = {
      accumulator.channel = value.channel
      accumulator.behavior = value.behavior
      accumulator.count += 1

      accumulator
    }

    override def getResult(accumulator: MarketingCountView): MarketingCountView = accumulator

    override def merge(a: MarketingCountView, b: MarketingCountView): MarketingCountView = {
      a.count += b.count
      a
    }
  }

}
