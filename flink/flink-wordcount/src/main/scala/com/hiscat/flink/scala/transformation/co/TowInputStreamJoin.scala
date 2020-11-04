package com.hiscat.flink.scala.transformation.co

import java.time.Duration
import java.util.Properties

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector
import org.apache.kafka.clients.consumer.ConsumerConfig

object TowInputStreamJoin {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val props = new Properties()
    props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092")
    props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "two.stream.join.test")
    val left = env
      .addSource(
        new FlinkKafkaConsumer[String]("left", new SimpleStringSchema(), props)
      )
      .map(e => {
        val split = e.split(",")
        (split(0), split(1))
      })
      .assignTimestampsAndWatermarks(
        WatermarkStrategy.forBoundedOutOfOrderness(Duration.ZERO)
          .withTimestampAssigner(
            new SerializableTimestampAssigner[(String, String)] {
              override def extractTimestamp(element: (String, String), recordTimestamp: Long): Long = {
                element._1.toLong
              }
            }
          )
      )
    val right = env
      .addSource(
        new FlinkKafkaConsumer[String]("right", new SimpleStringSchema(), props)
      )
      .map(e => {
        val split = e.split(",")
        (split(0), split(1))
      })
      .assignTimestampsAndWatermarks(
        WatermarkStrategy.forBoundedOutOfOrderness(Duration.ZERO)
          .withTimestampAssigner(
            new SerializableTimestampAssigner[(String, String)] {
              override def extractTimestamp(element: (String, String), recordTimestamp: Long): Long = {
                element._1.toLong
              }
            }
          )
      )

    left.connect(right)
      .keyBy(_._2, _._2)
      .process(
        new KeyedCoProcessFunction[String, (String, String), (String, String), (String, String)] {
          lazy private val left: ValueState[String] = getRuntimeContext.getState(
            new ValueStateDescriptor("left", classOf[String])
          )
          lazy private val right: ValueState[String] = getRuntimeContext.getState(
            new ValueStateDescriptor("right", classOf[String])
          )

          override def onTimer(timestamp: Long, ctx: KeyedCoProcessFunction[String, (String, String), (String, String), (String, String)]#OnTimerContext, out: Collector[(String, String)]): Unit = {

            left.clear()
            right.clear()
          }

          override def processElement1(value: (String, String), ctx: KeyedCoProcessFunction[String, (String, String), (String, String), (String, String)]#Context, out: Collector[(String, String)]): Unit = {
            println(s"left watermark:${ctx.timerService().currentWatermark()}")
            if (right.value() != null) {
              out.collect((value._2, right.value()))
              right.clear()
              left.clear()
            } else {
              left.update(value._2)
              ctx.timerService().registerEventTimeTimer(
                value._1.toLong + 3000L
              )
            }
          }

          override def processElement2(value: (String, String), ctx: KeyedCoProcessFunction[String, (String, String), (String, String), (String, String)]#Context, out: Collector[(String, String)]): Unit = {
            println(s"right watermark:${ctx.timerService().currentWatermark()}")
            if (left.value() != null) {
              out.collect((value._2, left.value()))
              right.clear()
              left.clear()
            } else {
              right.update(value._2)
              ctx.timerService().registerEventTimeTimer(
                value._1.toLong + 3000L
              )
            }
          }
        }
      )
      .print("join")

    env.execute("two input stream join")
  }
}
