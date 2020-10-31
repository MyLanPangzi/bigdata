package com.hiscat.network.flow.analysis

import java.sql.Timestamp
import java.time.format.DateTimeFormatter
import java.time.{Duration, LocalDateTime, ZoneOffset}

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api._
import org.apache.flink.streaming.api.datastream.DataStreamSink
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessAllWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.types.Row
import org.apache.flink.util.Collector
import org.redisson.Redisson
import org.redisson.api.RedissonClient
import org.redisson.config.{Config, SingleServerConfig}

object UvAnalysis {

  case class UserAccessEvent(userId: String, behavior: String, ts: Long)

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnv = StreamTableEnvironment.create(env)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val input = env.readTextFile("mock-data/UserBehavior.csv")
      .map(e => {
        val split = e.split(",")
        UserAccessEvent(
          split(0),
          split(3),
          split(4).toLong * 1000
        )
      })
      .filter(_.behavior == "pv")
      .assignTimestampsAndWatermarks(
        WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(60))
          .withTimestampAssigner(
            new SerializableTimestampAssigner[UserAccessEvent] {
              override def extractTimestamp(element: UserAccessEvent, recordTimestamp: Long): Long = element.ts
            }
          )
      )

    redisson(input)

    //    sql(tableEnv, input)
    //    tableApi(tableEnv, input)
    //    dataStreamApi(input)


    env.execute("uv analysis")
  }

  private def redisson(input: DataStream[UserAccessEvent]) = {
    input.timeWindowAll(Time.hours(1))
      .trigger(new EveryElementTrigger)
      .allowedLateness(Time.minutes(1))
      .process(new RedissonProcessFunction)
      .print("redisson")
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
            out.collect((new Timestamp(context.window.getEnd).toLocalDateTime, elements.map(_.userId).toSet.size))
          }
        }
      )
      .print("data stream")
  }

  class RedissonProcessFunction extends ProcessAllWindowFunction[UserAccessEvent, String, TimeWindow] {

    private var config: Config = _
    lazy private val client: RedissonClient = Redisson.create(config)
    lazy private val bloom: Bloom = Bloom(1 << 29)


    override def open(parameters: Configuration): Unit = {
      config = new Config()
      config.useSingleServer().setAddress("redis://hadoop102:6379")
    }

    override def process(context: Context,
                         elements: Iterable[UserAccessEvent],
                         out: Collector[String]): Unit = {
      val hash = bloom.hash(elements.head.userId, 70)
      val bitmap = client.getBitSet("uvBitmap")
      if (bitmap.get(hash)) {
        return
      }
      bitmap.set(hash)
      val map = client.getMap[Long, Long]("uvCountMap")
      val count = map.getOrDefault(context.window.getEnd, 0) + 1
      map.put(context.window.getEnd, count)
      out.collect(s"${new Timestamp(context.window.getEnd)}：$count")
    }

  }

  case class Bloom(size: Long) extends Serializable {
    def hash(value: String, seed: Int): Long = {
      var r = 0
      for (i <- 0 until value.length)
        r += seed + value.charAt(i)
      r & (size - 1)
    }
  }

  class EveryElementTrigger extends Trigger[UserAccessEvent, TimeWindow] {
    override def onElement(element: UserAccessEvent, timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.FIRE_AND_PURGE

    override def onProcessingTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE

    override def onEventTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE

    override def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = {}
  }


}
