package com.hiscat.realtime.analysis

import java.time.Duration
import com.hiscat.flink.sql.parser.{DmlCommand, SqlCommandParser}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.types.Row

object SqlJobSubmitter {

  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdp")

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironment.create(env)
    env.setParallelism(1)
    tEnv.getConfig.setIdleStateRetention(Duration.ofDays(1))
    tEnv.getConfig.getConfiguration.setString("table.optimizer.distinct-agg.split.enabled", "true")

    val commands = SqlCommandParser.getCommands(getClass.getResource("/test.sql").getPath)
    commands.filterNot(_.isInstanceOf[DmlCommand])
      .foreach(_.call(tEnv))

    val upsert = commands.filter(_.isInstanceOf[DmlCommand]).map(_.asInstanceOf[DmlCommand])
    if (upsert.nonEmpty) {
      val set = tEnv.createStatementSet()
      upsert.foreach(e => set.addInsertSql(e.sql))
      set.execute()
    }
//    test(tEnv,env)
  }

  def test(tEnv: StreamTableEnvironment, env: StreamExecutionEnvironment): Unit = {
    val table = tEnv.sqlQuery(
      """""".stripMargin)
    table.printSchema()
    table.toRetractStream[Row].print()
    env.fromElements(1).print()
    env.execute("test")
  }

}
