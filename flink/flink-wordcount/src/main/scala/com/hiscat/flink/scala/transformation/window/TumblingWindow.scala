package com.hiscat.flink.scala.transformation.window

import com.hiscat.flink.scala.{WordCount, WordSource}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

object TumblingWindow {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.addSource(new WordSource(1))
      .map(l => WordCount(l.line, l.ts, 1))
      .keyBy(_.word)
      //      .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
      .timeWindow(Time.seconds(5))
      .reduce(
        (x, y) => {
          x.count = x.count + y.count
          x.ts = x.ts.max(y.ts)
          x
        }
      )
      .print()

    env.execute("tumbling window")
  }
}
