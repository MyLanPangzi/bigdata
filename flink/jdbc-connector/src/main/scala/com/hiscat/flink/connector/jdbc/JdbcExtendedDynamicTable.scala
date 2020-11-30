package com.hiscat.flink.connector.jdbc

import java.sql.{PreparedStatement, Timestamp}

import org.apache.flink.connector.jdbc
import org.apache.flink.connector.jdbc.JdbcConnectionOptions.JdbcConnectionOptionsBuilder
import org.apache.flink.connector.jdbc.{JdbcConnectionOptions, JdbcSink, JdbcStatementBuilder}
import org.apache.flink.table.connector.ChangelogMode
import org.apache.flink.table.connector.sink.{DynamicTableSink, SinkFunctionProvider}
import org.apache.flink.table.data.{DecimalData, RowData, StringData, TimestampData}
import org.apache.flink.table.types.DataType
import org.apache.flink.types.RowKind

case class JdbcExtendedDynamicTable(
                                     options: JdbcConnectionOptions,
                                     fieldNames: Array[String],
                                     rowDataType: DataType
                                   ) extends DynamicTableSink {
  override def getChangelogMode(requestedMode: ChangelogMode): ChangelogMode =
    ChangelogMode.newBuilder()
      .addContainedKind(RowKind.DELETE)
      .addContainedKind(RowKind.INSERT)
      .addContainedKind(RowKind.UPDATE_AFTER)
      .build()

  override def getSinkRuntimeProvider(context: DynamicTableSink.Context): DynamicTableSink.SinkRuntimeProvider = {
    println("table")
    SinkFunctionProvider.of(JdbcSInkFunction(options, fieldNames, rowDataType))
    //    SinkFunctionProvider.of(
    //      JdbcSink.sink(
    //        if (options.enableUpsert)
    //          s"UPSERT INTO ${options.tableName} VALUES $parameters"
    //        else
    //          s"INSERT INTO ${options.tableName} VALUES $parameters"
    //        ,
    //        getJdbcStatementBuilder,
    //        new JdbcConnectionOptionsBuilder()
    //          .withUrl(options.url)
    //          .withDriverName(options.driver)
    //          .withUsername(options.username.orNull)
    //          .withPassword(options.password.orNull)
    //          .build()
    //      )
    //    )
  }

  /*
    private def getJdbcStatementBuilder: JdbcStatementBuilder[RowData] = {
      new JdbcStatementBuilder[RowData] {
        override def accept(p: PreparedStatement, r: RowData): Unit = {
          println(r)
          p.getConnection.setAutoCommit(true)
          import scala.collection.JavaConverters._
          rowDataType.getChildren.asScala.zipWithIndex
            .map(e => RowData.createFieldGetter(e._1.getLogicalType, e._2))
            .map(g => g.getFieldOrNull(r))
            .map {
              case e: StringData => new String(e.toBytes)
              case e: DecimalData => e.toBigDecimal
              case e: TimestampData => new Timestamp(e.getMillisecond)
              case e => e
            }.zipWithIndex
            .foreach {
              case (value, i) => p.setObject(i + 1, value)
            }
        }
      }
    }
  */

  override def copy(): DynamicTableSink = JdbcExtendedDynamicTable(options, fieldNames, rowDataType)

  override def asSummaryString(): String = "jdbc extended table sink"
}
