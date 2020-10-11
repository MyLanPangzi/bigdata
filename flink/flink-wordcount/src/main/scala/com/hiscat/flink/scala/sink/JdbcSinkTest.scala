package com.hiscat.flink.scala.sink

import java.sql.PreparedStatement

import com.hiscat.flink.scala.WordCount
import org.apache.flink.connector.jdbc.{JdbcConnectionOptions, JdbcSink, JdbcStatementBuilder}
import org.apache.flink.streaming.api.scala._

object JdbcSinkTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.fromElements(
      WordCount("hello", 1, 1),
      WordCount("world", 1, 1),
    )
      .addSink(JdbcSink.sink[WordCount](
        "replace into wc (word, count, ts) values (?,?,?)   ",
    //noinspection ConvertExpressionToSAM
        new JdbcStatementBuilder[WordCount] {
          override def accept(ps: PreparedStatement, w: WordCount): Unit = {
            ps.setString(1, w.word)
            ps.setInt(2, w.count.toInt)
            ps.setLong(3, w.ts)
          }
        }
        ,
        new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
          .withUrl("jdbc:mysql://hadoop102:/flink")
          .withDriverName("com.mysql.jdbc.Driver")
          .withUsername("root")
          .withPassword("000000")
          .build()
      )
      );

    env.execute("redis sink")
  }


}
