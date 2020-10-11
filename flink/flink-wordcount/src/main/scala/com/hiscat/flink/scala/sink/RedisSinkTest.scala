package com.hiscat.flink.scala.sink

import com.hiscat.flink.scala.WordCount
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig

import org.apache.flink.streaming.connectors.redis.common.mapper._

object RedisSinkTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.fromElements(
      WordCount("hello", 1, 1),
      WordCount("world", 1, 1),
    )
      .addSink(new RedisSink[WordCount](
        new FlinkJedisPoolConfig.Builder()
          .setHost("hadoop102")
          .build,
        new WordCountMapper())
      )

    env.execute("redis sink")
  }


  class WordCountMapper extends RedisMapper[WordCount] {
    override def getCommandDescription = new RedisCommandDescription(RedisCommand.HSET, "wc")

    override def getKeyFromData(data: WordCount): String = {
      data.word
    }

    override def getValueFromData(data: WordCount): String = {
      data.toString
    }
  }

}
