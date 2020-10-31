package com.hiscat.network.flow.analysis

import java.sql.Timestamp
import java.time.format.DateTimeFormatter
import java.time.{Duration, LocalDateTime, ZoneOffset}
import java.util.concurrent.TimeUnit

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.datastream.DataStreamSink
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.api._
import org.apache.flink.types.Row
import org.apache.flink.util.Collector

import scala.math.{Ordering, abs}

object PvAnalysis {

  case class ApacheEvenLog(url: String, ts: Long)

  case class UrlCountView(var url: String, var count: Long, var ts: Long)

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    val tableEnv = StreamTableEnvironment.create(env)

    val input = env.readTextFile("mock-data/apache.log")
      .map(e => {
        val split = e.split(" ")
        val milli = LocalDateTime.parse(split(split.length - 4), DateTimeFormatter.ofPattern("dd/MM/yyyy:HH:mm:ss"))
          .toInstant(ZoneOffset.ofHours(8))
          .toEpochMilli
        val url = split.last
        ApacheEvenLog(url.split("\\?")(0), milli)
      })
      .assignTimestampsAndWatermarks(
        WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(60))
          .withTimestampAssigner(new SerializableTimestampAssigner[ApacheEvenLog] {
            override def extractTimestamp(element: ApacheEvenLog, recordTimestamp: Long): Long = element.ts
          })
      )
      //      .assignAscendingTimestamps(_.ts)
      .filter(data => {
        val pattern = "^((?!\\.(css|js|png|ico)$).)*$".r
        (pattern findFirstIn data.url).nonEmpty
      })

    sql(tableEnv, input)

    //    tableApi(tableEnv, input)

    //    dataStreamApi(input)

    env.execute("network flow analysis")
  }

  def sql(tableEnv: StreamTableEnvironment, input: DataStream[ApacheEvenLog]): DataStreamSink[(Boolean, Row)] = {
    tableEnv.createTemporaryView("event", input, $"ts".rowtime, $"url")
    tableEnv.sqlQuery(
      """
        |SELECT t.wend, t.url, t.cnt
        |FROM (
          |SELECT *,ROW_NUMBER() OVER(PARTITION BY wend ORDER BY cnt) rnum
          |FROM (
            |SELECT HOP_END(ts,INTERVAL '5' SECOND, INTERVAL '10' MINUTE) wend, url, count(*) cnt
            |FROM event
            |GROUP BY url,HOP(ts,INTERVAL '5' SECOND, INTERVAL '10' MINUTE)
          |)t
        |)t where t.rnum <=3
        |""".stripMargin)
      .toRetractStream[Row]
      .print("sql")

  }

  private def tableApi(tableEnv: StreamTableEnvironment, input: DataStream[ApacheEvenLog]): DataStreamSink[(Boolean, Row)] = {
    val table = tableEnv.fromDataStream(input, $"url", $"ts".rowtime)

    val aggTable = table
      .window(Slide over 10.minute every 5.second on $"ts" as $"w")
      .groupBy($"w", $"url")
      .select($"w".end as "wend", $"url", $"url".count as "cnt")

    //    aggTable.toAppendStream[Row].print("table")

    tableEnv.createTemporaryView("event", aggTable, $"wend", $"url", $"cnt")
    tableEnv.sqlQuery(
      """
        |SELECT t.wend,t.url,t.cnt
        |FROM (
        | SELECT *,ROW_NUMBER() OVER(PARTITION BY wend ORDER BY cnt DESC) num
        | FROM event
        |)t
        |WHERE t.num <=3
        |""".stripMargin)
      .toRetractStream[Row]
      .print("table")
  }

  private def dataStreamApi(input: DataStream[ApacheEvenLog]): DataStreamSink[String] = {
    input
      .keyBy(_.url)
      .timeWindow(Time.minutes(10), Time.seconds(5))
      .allowedLateness(Time.minutes(1))
      .aggregate(
        new UrlCountAgg(),
        (_: String, w: TimeWindow, v: Iterable[UrlCountView], out: Collector[UrlCountView]) => {
          v.head.ts = w.getEnd
          out.collect(v.head)
        }
      )
      .keyBy(_.ts)
      .process(new TopN(3))
      .print("test")
  }

  class UrlCountAgg extends AggregateFunction[ApacheEvenLog, UrlCountView, UrlCountView] {
    override def createAccumulator(): UrlCountView = UrlCountView("", 0, 0)

    override def add(value: ApacheEvenLog, accumulator: UrlCountView): UrlCountView = {
      accumulator.count = accumulator.count + 1
      accumulator.url = value.url
      accumulator
    }

    override def getResult(accumulator: UrlCountView): UrlCountView = accumulator

    override def merge(a: UrlCountView, b: UrlCountView): UrlCountView = {
      a.count += b.count
      a
    }
  }


  class TopN(i: Int) extends KeyedProcessFunction[Long, UrlCountView, String] {

    lazy val urlCount: MapState[String, Long] = getRuntimeContext.getMapState[String, Long](
      new MapStateDescriptor[String, Long]("urlCount", classOf[String], classOf[Long])
    )

    override def processElement(value: UrlCountView,
                                ctx: KeyedProcessFunction[Long, UrlCountView, String]#Context,
                                out: Collector[String]): Unit = {
      urlCount.put(value.url, value.count)

      ctx.timerService().registerEventTimeTimer(
        ctx.getCurrentKey + 1
      )
      ctx.timerService().registerEventTimeTimer(
        ctx.getCurrentKey + TimeUnit.MINUTES.toMillis(10)
      )
    }

    override def onTimer(timestamp: Long,
                         ctx: KeyedProcessFunction[Long, UrlCountView, String]#OnTimerContext,
                         out: Collector[String]): Unit = {
      if (ctx.getCurrentKey + 1 == timestamp) {
        import scala.collection.JavaConverters._
        val top = urlCount
          .entries()
          .asScala.toList
          .sortBy(_.getValue)(Ordering.Long.reverse)
          .take(i)
          .mkString("\n")
        TimeUnit.SECONDS.sleep(1)
        out.collect(
          s"""
             |${new Timestamp(timestamp)}
             |${top}
             |""".stripMargin)
      } else
        urlCount.clear()
    }
  }


}
