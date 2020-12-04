package com.hiscat.flink.connector.jdbc

import java.sql.PreparedStatement

import org.apache.flink.connector.jdbc.JdbcConnectionOptions.JdbcConnectionOptionsBuilder
import org.apache.flink.connector.jdbc.{JdbcSink, JdbcStatementBuilder}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.bridge.scala._
//import org.apache.phoenix.jdbc.PhoenixDriver
import org.scalatest.{BeforeAndAfter, BeforeAndAfterEach}
import org.scalatest.funsuite.AnyFunSuite

class PhoenixTest extends AnyFunSuite with BeforeAndAfter with BeforeAndAfterEach {
  lazy val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
  lazy private val tEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)

  test("phoenix upsert") {
    env.fromElements((1L, "flink"))
      .addSink(
        JdbcSink.sink(
          "UPSERT INTO student VALUES(?,?)",
          new JdbcStatementBuilder[(Long, String)] {
            override def accept(p: PreparedStatement, e: (Long, String)): Unit = {
              p.getConnection.setAutoCommit(true)
              p.setLong(1, e._1)
              p.setString(2, e._2)
            }
          },
          new JdbcConnectionOptionsBuilder()
            .withUrl("jdbc:phoenix:hadoop102:2181/default")
//            .withDriverName(classOf[PhoenixDriver].getName)
            .build()
        )
      )
  }

  override protected def afterEach(): Unit = {
    env.execute("phoenix test")
  }
}
