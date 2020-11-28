package com.hiscat.realtime.analysis

import com.hiscat.flink.sql.parser.{DmlCommand, SqlCommandParser}
import org.apache.flink.api.common.time.Time
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.types.Row

object DauJob {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnv = StreamTableEnvironment.create(env)
    tableEnv.getConfig.setIdleStateRetentionTime(Time.hours(12), Time.hours(24))
    tableEnv.getConfig.getConfiguration.setString("table.optimizer.distinct-agg.split.enabled", "true");

    val commands = SqlCommandParser.getCommands(getClass.getResource("/start_event.sql").getPath)
    commands.filterNot(_.isInstanceOf[DmlCommand])
      .foreach(_.call(tableEnv))
    val set = tableEnv.createStatementSet()
    commands.filter(_.isInstanceOf[DmlCommand])
      .map(_.asInstanceOf[DmlCommand])
      .foreach(e => set.addInsertSql(e.sql))

    set.execute()
  }
}
