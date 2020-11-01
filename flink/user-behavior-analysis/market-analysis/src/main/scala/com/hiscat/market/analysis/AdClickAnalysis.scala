package com.hiscat.market.analysis

import java.time.temporal.ChronoField
import java.time.{LocalDate, LocalDateTime, ZoneOffset}

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.types.Row
import org.apache.flink.util.Collector

object AdClickAnalysis {

  case class AdClickEvent(uid: String, adId: String, province: String, city: String, ts: Long)

  case class ProvinceCountView(var windowEnd: LocalDateTime, var province: String, var count: Long)

  val WARN = new OutputTag[AdClickEvent]("warn")

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnv = StreamTableEnvironment.create(env)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val input = env.readTextFile("mock-data/AdClickLog.csv")
      .map(e => {
        val split = e.split(",")
        AdClickEvent(split(0), split(1), split(2), split(3), split(4).toLong)
      })
      .assignAscendingTimestamps(_.ts * 1000)
      .keyBy(e => (e.uid, e.adId))
      .process(new FilterBlockListKeyedProcessFunction)

    sql(tableEnv, input)
    //    table(tableEnv, input)
    //    stream(input)

    env.execute("ad click analysis")
  }

  private def sql(tableEnv: StreamTableEnvironment, input: DataStream[AdClickEvent]) = {
    tableEnv.createTemporaryView("event", input, $"province", $"ts".rowtime)
    tableEnv.sqlQuery(
      """
        |SELECT HOP_END(ts, INTERVAL '5' SECOND, INTERVAL '1' HOUR), province, count(*)
        |FROM event
        |GROUP BY HOP(ts, INTERVAL '5' SECOND, INTERVAL '1' HOUR), province
        |""".stripMargin)
      .toAppendStream[Row]
      .print("sql")
  }

  private def table(tableEnv: StreamTableEnvironment, input: DataStream[AdClickEvent]) = {
    tableEnv.fromDataStream(input, $"province", $"ts".rowtime)
      .window(Slide over 1.hour every 5.second on $"ts" as $"w")
      .groupBy($"w", $"province")
      .select($"w".end, $"province", $"province".count)
      .toAppendStream[Row]
      .print("table")
  }

  private def stream(input: DataStream[AdClickEvent]) = {
    input.keyBy(_.province)
      .timeWindow(Time.hours(1), Time.seconds(5))
      .aggregate(
        new ProvinceCountAgg,
        (_: String, w: TimeWindow, v: Iterable[ProvinceCountView], out: Collector[ProvinceCountView]) => {
          v.head.windowEnd = LocalDateTime.ofEpochSecond(w.getEnd / 1000, 1, ZoneOffset.ofHours(8))
          out.collect(v.head)
        }
      )
      .print("stream")
  }

  class ProvinceCountAgg extends AggregateFunction[AdClickEvent, ProvinceCountView, ProvinceCountView] {
    override def createAccumulator(): ProvinceCountView = ProvinceCountView(null, "", 0)

    override def add(value: AdClickEvent, accumulator: ProvinceCountView): ProvinceCountView = {
      accumulator.province = value.province
      accumulator.count += 1
      accumulator
    }

    override def getResult(accumulator: ProvinceCountView): ProvinceCountView = accumulator

    override def merge(a: ProvinceCountView, b: ProvinceCountView): ProvinceCountView = {
      a.count += b.count
      a
    }
  }

  class FilterBlockListKeyedProcessFunction extends KeyedProcessFunction[(String, String), AdClickEvent, AdClickEvent] {

    lazy private val userAdCount: MapState[String, Long] = getRuntimeContext.getMapState(
      new MapStateDescriptor("userAdCount", classOf[String], classOf[Long])
    )

    override def processElement(value: AdClickEvent,
                                ctx: KeyedProcessFunction[(String, String), AdClickEvent, AdClickEvent]#Context,
                                out: Collector[AdClickEvent]): Unit = {
      var count = 1L
      if (userAdCount.contains(value.uid)) {
        count = userAdCount.get(value.uid) + 1
      }
      userAdCount.put(value.uid, count)
      if (count < 100) {
        out.collect(value)
      } else if (count == 100) {
        ctx.output[AdClickEvent](WARN, value)
      }
      ctx.timerService().registerProcessingTimeTimer(
        LocalDate.now()
          .plusDays(1)
          .atTime(0, 0)
          .toInstant(ZoneOffset.ofHours(8))
          .toEpochMilli
      )
    }

    override def onTimer(timestamp: Long,
                         ctx: KeyedProcessFunction[(String, String), AdClickEvent, AdClickEvent]#OnTimerContext,
                         out: Collector[AdClickEvent]): Unit = {
      userAdCount.clear()
    }
  }


}
