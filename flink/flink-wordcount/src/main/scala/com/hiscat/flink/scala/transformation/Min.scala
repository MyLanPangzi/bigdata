package com.hiscat.flink.scala.transformation

import com.hiscat.flink.scala.{WordCount, WordSource}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * 只返回元素某个字段的最小值，不修改其余字段
 */
object Min {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    env.fromElements(
      WordCount("hello", 1, 1),
      WordCount("hello", 2, 0),
      WordCount("world", 3, 1),
      WordCount("world", 4, 0),
    )
      .keyBy(_.word)
      .min("count")
      .print()

    env.execute("min transformation")
  }
}
