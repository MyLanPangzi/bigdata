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
    tEnv.getConfig.getConfiguration.setString("table.optimizer.distinct-agg.split.enabled", "true");

    //    val commands = SqlCommandParser.getCommands(getClass.getResource("/test.sql").getPath)
    //    commands.filterNot(_.isInstanceOf[DmlCommand])
    //      .foreach(_.call(tEnv))
    //
    //    val upsert = commands.filter(_.isInstanceOf[DmlCommand]).map(_.asInstanceOf[DmlCommand])
    //    if (upsert.nonEmpty) {
    //      val set = tEnv.createStatementSet()
    //      upsert.foreach(e => set.addInsertSql(e.sql))
    //      set.execute()
    //    }
    registerUserTable(tEnv)
    registerPrintTable(tEnv)

    val table = tEnv.sqlQuery(
      """
        |SELECT *
        |FROM user_info
        |""".stripMargin)
    table.executeInsert("print")
//    table.toRetractStream[Row].print()
    env.fromElements(1).print()
    env.execute("test")
  }

  private def registerUserTable(tEnv: StreamTableEnvironment) = {
    tEnv.executeSql(
      """
        |CREATE TABLE user_info
        |(
        |	id bigint comment '编号',
        |	login_name varchar(200)  comment '用户名称',
        |	nick_name varchar(200)  comment '用户昵称',
        |	passwd varchar(200)  comment '用户密码',
        |	name varchar(200)  comment '用户姓名',
        |	phone_num varchar(200)  comment '手机号',
        |	email varchar(200)  comment '邮箱',
        |	head_img varchar(200)  comment '头像',
        |	user_level varchar(200)  comment '用户级别',
        |	birthday date  comment '用户生日',
        |	gender varchar(1)  comment '性别 M男,F女',
        |	create_time TIMESTAMP comment '创建时间',
        |	operate_time TIMESTAMP comment '修改时间',
        |	 primary key(id) not enforced
        |)
        | WITH (
        | 'connector' = 'mysql-cdc',
        | 'hostname' = 'hadoop102',
        | 'port' = '3306',
        | 'username' = 'root',
        | 'password' = '000000',
        | 'database-name' = 'gmall2020',
        | 'table-name' = 'user_info'
        |)
        |""".stripMargin)
  }

  private def registerPrintTable(tEnv: StreamTableEnvironment) = {
    tEnv.executeSql(
      """
        |CREATE TABLE print
        |(
        |	id bigint comment '编号',
        |	login_name varchar(200)  comment '用户名称',
        |	nick_name varchar(200)  comment '用户昵称',
        |	passwd varchar(200)  comment '用户密码',
        |	name varchar(200)  comment '用户姓名',
        |	phone_num varchar(200)  comment '手机号',
        |	email varchar(200)  comment '邮箱',
        |	head_img varchar(200)  comment '头像',
        |	user_level varchar(200)  comment '用户级别',
        |	birthday date  comment '用户生日',
        |	gender varchar(1)  comment '性别 M男,F女',
        |	create_time TIMESTAMP comment '创建时间',
        |	operate_time TIMESTAMP comment '修改时间',
        |	 primary key(id) not enforced
        |)
        | WITH (
        | 'connector' = 'print'
        |)
        |""".stripMargin)
  }

}
