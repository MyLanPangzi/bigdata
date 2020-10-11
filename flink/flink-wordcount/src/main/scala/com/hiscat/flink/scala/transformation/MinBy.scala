package com.hiscat.flink.scala.transformation

import com.hiscat.flink.scala.{WordCount, WordSource}
import org.apache.flink.streaming.api.scala._

/**
 * 返回字段值最小的元素
 */
object MinBy {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    env
      .fromCollection(
        List(
          WordCount("hello", 1, 1),
          WordCount("hello", 2, 0),
          WordCount("flink", 3, 1),
          WordCount("flink", 4, 0),
        )
      )
      .keyBy(_.word)
      .minBy("count")
      .print()

    env.execute("minBy transformation")
  }
}
