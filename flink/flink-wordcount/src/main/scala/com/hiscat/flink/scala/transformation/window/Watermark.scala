package com.hiscat.flink.scala.transformation.window

import com.hiscat.flink.scala.{Sensor, SensorSource}
import org.apache.flink.api.common.eventtime._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._

object Watermark {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.getConfig.setAutoWatermarkInterval(500)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)


    env.addSource(new SensorSource)
      .assignTimestampsAndWatermarks(new PunctuatedWatermarkStrategy)
      //      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessWatermarkStrategy(3000))
      //      .assignTimestampsAndWatermarks(TimeLagWatermarkGenerator(5000L))
      //      .assignTimestampsAndWatermarks(
      //        WatermarkStrategy.forMonotonousTimestamps[Sensor]()
      ////          .withTimestampAssigner(_ => (e: Sensor, _: Long) => e.ts) // stupid scala ,cannot infer the result type
      //          .withTimestampAssigner(new SerializableTimestampAssigner[Sensor] {
      //            override def extractTimestamp(element: Sensor, recordTimestamp: Long): Long = element.ts
      //          })
      //          .withIdleness(Duration.ofSeconds(10))
      //      )
      //      .assignTimestampsAndWatermarks(WatermarkStrategy.noWatermarks[Sensor]())
      //      .assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness[Sensor](Duration.ofSeconds(0)))
      //      .assignTimestampsAndWatermarks(WatermarkStrategy.forGenerator[Sensor](_ => new BoundedOutOfOrdernessWatermarks[Sensor](Duration.ofSeconds(0))))

      //            .assignAscendingTimestamps(e => e.ts)
      //      .assignTimestampsAndWatermarks(new Strategy[Sensor](
      //        new BoundedOutOfOrdernessTimestampExtractor[Sensor](Time.seconds(0)) {
      //          override def extractTimestamp(element: Sensor): Long = element.ts
      //        }
      //      ))
      .print()

    env.execute("watermark")
  }

  class BoundedOutOfOrdernessWatermarkStrategy(maxOutOfOrderness: Long = 0L) extends WatermarkStrategy[Sensor] {

    override def createTimestampAssigner(context: TimestampAssignerSupplier.Context): TimestampAssigner[Sensor] =
      (element: Sensor, _: Long) => element.ts

    override def createWatermarkGenerator(context: WatermarkGeneratorSupplier.Context): WatermarkGenerator[Sensor] =
      new WatermarkGenerator[Sensor] {
        var currentMaxTimestamp: Long = _

        override def onEvent(event: Sensor, eventTimestamp: Long, output: WatermarkOutput): Unit =
          currentMaxTimestamp = currentMaxTimestamp.max(eventTimestamp)

        override def onPeriodicEmit(output: WatermarkOutput): Unit = {
          println(System.currentTimeMillis())
          output.emitWatermark(new Watermark(currentMaxTimestamp - maxOutOfOrderness - 1))
        }
      }
  }

  case class TimeLagWatermarkStrategy(timeLag: Long) extends WatermarkStrategy[Sensor] {

    override def createTimestampAssigner(context: TimestampAssignerSupplier.Context): TimestampAssigner[Sensor] =
      (element: Sensor, _: Long) => element.ts

    override def createWatermarkGenerator(context: WatermarkGeneratorSupplier.Context): WatermarkGenerator[Sensor] =
      new WatermarkGenerator[Sensor] {
        override def onEvent(event: Sensor, eventTimestamp: Long, output: WatermarkOutput): Unit = {}

        override def onPeriodicEmit(output: WatermarkOutput): Unit = {
          println(System.currentTimeMillis())
          output.emitWatermark(new Watermark(System.currentTimeMillis() - timeLag))
        }
      }
  }


  case class PunctuatedWatermarkStrategy() extends WatermarkStrategy[Sensor] {

    override def createTimestampAssigner(context: TimestampAssignerSupplier.Context): TimestampAssigner[Sensor] =
      (element: Sensor, _: Long) => element.ts

    override def createWatermarkGenerator(context: WatermarkGeneratorSupplier.Context): WatermarkGenerator[Sensor] =
      new WatermarkGenerator[Sensor] {

        var maxTimestamp: Long = _

        override def onEvent(event: Sensor, eventTimestamp: Long, output: WatermarkOutput): Unit = {
          maxTimestamp = eventTimestamp.max(maxTimestamp)
          if (event.id.startsWith("sensor_1")) {
            output.emitWatermark(new Watermark(maxTimestamp))
          }
        }

        override def onPeriodicEmit(output: WatermarkOutput): Unit = {
          println(System.currentTimeMillis())
        }
      }
  }

}
