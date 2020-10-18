package com.hiscat.flink

import java.util.Random
import java.util.concurrent.TimeUnit

import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}

package object scala {

  case class Line(line: String, ts: Long)

  case class Sensor(var id: String, var ts: Long, var temperature: Double)

  case class WordCount(var word: String, var ts: Long, var count: Long)

  class WordSource(latency: Int = 3) extends RichParallelSourceFunction[Line] {

    var running = true

    val words = List("Hello", "world", "flink", "scala", "hadoop", "spark", "hive", "flume", "kafka")

    override def run(ctx: SourceFunction.SourceContext[Line]): Unit = {
      val random = new Random()
      while (running) {
        ctx.collect(Line(words(random.nextInt(words.size)), System.currentTimeMillis()))
        TimeUnit.SECONDS.sleep(latency)
      }

    }

    override def cancel(): Unit = running = false
  }

  class SensorSource extends RichParallelSourceFunction[Sensor] {
    private var running = true

    override def run(ctx: SourceFunction.SourceContext[Sensor]): Unit = {

      val random = new Random()
      val sensors = (1 to 10).map(i => Sensor(s"sensor_$i", System.currentTimeMillis(), random.nextDouble() + 20))
      while (running) {
        sensors.map(s => Sensor(s.id, System.currentTimeMillis(), s.temperature + random.nextGaussian()))
          .foreach(e => {
            ctx.collect(e)
            TimeUnit.SECONDS.sleep(1)
          })
      }
    }

    override def cancel(): Unit = running = false
  }

}
