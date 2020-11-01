package com.hiscat.login.failure.detect

import _root_.java.time.{Duration, LocalDateTime, ZoneOffset}
import java.util

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.cep.functions.PatternProcessFunction
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

object LoginFailureDetect {

  case class LoginEvent(uid: String, ip: String, eventType: String, ts: Long)

  case class Warn(uid: String, start: LocalDateTime, end: LocalDateTime, ip: Iterable[String])

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val input = env.readTextFile("mock-data/LoginLog.csv")
      .map(e => {
        val split = e.split(",")
        LoginEvent(split(0), split(1), split(2), split(3).toLong)
      })
      .assignTimestampsAndWatermarks(
        WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofMinutes(1))
          .withTimestampAssigner(
            new SerializableTimestampAssigner[LoginEvent] {
              override def extractTimestamp(element: LoginEvent, recordTimestamp: Long): Long = element.ts * 1000
            }
          )
      )
      .keyBy(_.uid)
    val pattern = Pattern.begin[LoginEvent]("start").where(_.eventType == "fail")
      .next("next").where(_.eventType == "fail")
      .within(Time.seconds(2))


    CEP.pattern(input, pattern)
      .process(
        (map: util.Map[String, util.List[LoginEvent]], _: PatternProcessFunction.Context, collector: Collector[Warn]) => {
          import scala.collection.JavaConverters._
          val start = map.get("start").asScala.head
          val end = map.get("next").asScala.head
          collector.collect(
            Warn(
              start.uid,
              LocalDateTime.ofEpochSecond(start.ts, 0, ZoneOffset.ofHours(8)),
              LocalDateTime.ofEpochSecond(end.ts, 0, ZoneOffset.ofHours(8)),
              List(start.ip, end.ip)
            )
          )
        }
      )
      .print("cep")


    env.execute("login failure detect")
  }
}
