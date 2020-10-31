package com.hiscat.network.flow.analysis

import java.sql.Timestamp
import java.time.format.DateTimeFormatter
import java.time.{Duration, LocalDateTime, ZoneOffset}

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.streaming.api._
import org.apache.flink.streaming.api.datastream.DataStreamSink
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessAllWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.types.Row
import org.apache.flink.util.Collector

object UvAnalysis {

  case class UserAccessEvent(ip: String, ts: Long)

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnv = StreamTableEnvironment.create(env)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val input = env.readTextFile("mock-data/apache.log")
      .map(e => {
        val split = e.split(" ")
        UserAccessEvent(
          split(0),
          LocalDateTime.parse(split(3), DateTimeFormatter.ofPattern("dd/MM/yyyy:HH:mm:ss"))
            .toInstant(ZoneOffset.ofHours(8))
            .toEpochMilli
        )
      })
      .assignTimestampsAndWatermarks(
        WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(60))
          .withTimestampAssigner(
            new SerializableTimestampAssigner[UserAccessEvent] {
              override def extractTimestamp(element: UserAccessEvent, recordTimestamp: Long): Long = element.ts
            }
          )
      )
    sql(tableEnv, input)


    //    tableApi(tableEnv, input)

    //        dataStreamApi(input)


    env.execute("uv analysis")
  }

  private def sql(tableEnv: StreamTableEnvironment, input: DataStream[UserAccessEvent]): DataStreamSink[(Boolean, Row)] = {
    tableEnv.createTemporaryView("event", input, $"ts".rowtime, $"ip")
    tableEnv.sqlQuery(
      """
        |SELECT wend,count(*) cnt
        |FROM (
          |SELECT TUMBLE_END(ts, INTERVAL '1' hour) wend, ip
          |FROM event
          |GROUP BY ip, TUMBLE(ts, INTERVAL '1' hour)
        |)t
        |GROUP BY wend
        |""".stripMargin)
      .toRetractStream[Row]
      .print("sql")
  }

  private def tableApi(tableEnv: StreamTableEnvironment, input: DataStream[UserAccessEvent]): Unit = {
    val table = tableEnv.fromDataStream(input, $"ip", $"ts".rowtime)
      .window(Tumble over 1.hour on $"ts" as $"w")
      .groupBy($"w", $"ip")
      .select($"w".end as "wend", $"ip")
      .groupBy($"wend")
      .select($"wend", $"ip".count as "cnt")
      .toRetractStream[Row]
      .print("table")
  }

  private def dataStreamApi(input: DataStream[UserAccessEvent]): DataStreamSink[(LocalDateTime, Long)] = {
    input
      .timeWindowAll(Time.hours(1))
      .allowedLateness(Time.hours(1))
      .process(
        new ProcessAllWindowFunction[UserAccessEvent, (LocalDateTime, Long), TimeWindow] {
          override def process(context: Context,
                               elements: Iterable[UserAccessEvent],
                               out: Collector[(LocalDateTime, Long)]): Unit = {
            out.collect((new Timestamp(context.window.getEnd).toLocalDateTime, elements.map(_.ip).toSet.size))
          }
        }
      )
      .print("data stream")
  }
}
