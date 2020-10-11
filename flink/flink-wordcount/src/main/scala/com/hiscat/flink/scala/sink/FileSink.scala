package com.hiscat.flink.scala.sink

import com.hiscat.flink.scala.WordCount
import org.apache.flink.api.common.serialization.SimpleStringEncoder
import org.apache.flink.core.fs.Path
import org.apache.flink.runtime.state.memory.MemoryStateBackend
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy
import org.apache.flink.streaming.api.scala._

object FileSink {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.enableCheckpointing(10)
    env.setStateBackend(new MemoryStateBackend())

    env.fromElements(
      WordCount("hello", 1, 1),
      WordCount("world", 1, 1),
    )
      .addSink(
        StreamingFileSink.forRowFormat(
          new Path("output/word_count"),
          new SimpleStringEncoder[WordCount]()
        )
          .withRollingPolicy(
            DefaultRollingPolicy.builder()
              .withMaxPartSize(1024)
              .withRolloverInterval(1)
              .withInactivityInterval(1)
              .build[WordCount, String]()
          )
          .build()
      )

    env.execute("streaming file sink")
  }
}
