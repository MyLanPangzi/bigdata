package com.hiscat.flink.scala.transformation

import com.hiscat.flink.scala.WordCount
import org.apache.flink.api.common.functions.RichReduceFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.extensions._

object Reduce {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    env.fromElements(
      WordCount("hello", 1, 1),
      WordCount("hello", 2, 1),
      WordCount("flink", 3, 1),
      WordCount("flink", 4, 1),
      WordCount("flume", 5, 1),
      WordCount("hadoop", 6, 1),
      WordCount("hadoop", 7, 1),
    )
      .keyBy(_.word)
      .reduceWith {
        case (x, y) =>
          x.count += y.count
          x.ts = x.ts.max(y.ts)
          x
      }
      .print()

    env.execute("reduce transformation")
  }
}
