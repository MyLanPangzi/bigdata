package com.hiscat.flink.scala.transformation.window

import com.hiscat.flink.scala.{WordCount, WordSource}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.{SlidingProcessingTimeWindows, TumblingProcessingTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object SlidingWindow {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.addSource(new WordSource(2))
      .map(l => WordCount(l.line, l.ts, 1))
      .keyBy(_.word)
      //      .window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(5)))
      .timeWindow(Time.seconds(10), Time.seconds(5))
      .reduce(
        (x, y) => {
          x.count = x.count + y.count
          x
        },
        (k: String, w: TimeWindow, it: Iterable[WordCount], out: Collector[WordCount]) => {
          out.collect(WordCount(k, w.getEnd, it.head.count))
        }
      )
      .print()

    env.execute("slide window")
  }
}
