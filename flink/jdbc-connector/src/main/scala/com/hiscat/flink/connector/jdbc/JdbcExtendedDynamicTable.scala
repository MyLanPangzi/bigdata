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
    SinkFunctionProvider.of(new JavaJdbcSInkFunction)
  }

  override def copy(): DynamicTableSink = JdbcExtendedDynamicTable(options, fieldNames, rowDataType)

  override def asSummaryString(): String = "jdbc extended table sink"
}
