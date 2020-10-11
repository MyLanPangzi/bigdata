package com.hiscat.flink

import java.util.Random
import java.util.concurrent.TimeUnit

import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}

package object scala {

  case class Line(line: String, ts: Long)

  case class WordCount(var word: String, var ts: Long, var count: Long)

  class WordSource(latency: Int = 3) extends RichParallelSourceFunction[Line] {

    var running = true

    val words = List("world", "flink", "scala", "hadoop", "spark", "hive", "flume", "kafka")

    override def run(ctx: SourceFunction.SourceContext[Line]): Unit = {
      val random = new Random()
      while (running) {
        ctx.collect(Line("hello " + words(random.nextInt(words.size)), System.currentTimeMillis()))
        TimeUnit.SECONDS.sleep(latency)
      }

    }

    override def cancel(): Unit = running = false
  }


}
