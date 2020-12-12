package com.hiscat.realtime.analysis

import java.time.Duration

import com.hiscat.flink.sql.parser.{DmlCommand, SqlCommandParser}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.types.Row

object SqlJobSubmitter {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironment.create(env)
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
      """select p.id,
        |       (name,
        |        area_code,
        |        iso_code,
        |        region_id,
        |        region_name)
        |from base_province p
        |         left join base_region r on p.region_id = r.id
        |         """.stripMargin)
    table.toRetractStream[Row].print()
    table.executeInsert("hbase_dim_base_province")
    val t2 = tEnv.sqlQuery(
      """
        |select user_id, ROW(if_first_order) cf
        |from first_order_view
        |where if_first_order
        |""".stripMargin)
    t2.executeInsert("hbase_user_first_order_status")
    env.fromElements(1).print()
    env.execute("test")
  }

}
