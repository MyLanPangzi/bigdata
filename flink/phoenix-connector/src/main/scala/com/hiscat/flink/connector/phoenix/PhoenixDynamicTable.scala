package com.hiscat.flink.connector.phoenix

import org.apache.flink.table.connector.ChangelogMode
import org.apache.flink.table.connector.sink.{DynamicTableSink, SinkFunctionProvider}
import org.apache.flink.table.types.DataType
import org.apache.flink.types.RowKind

case class PhoenixDynamicTable(
                                options: PhoenixOptions,
                                fieldDataTypes: Array[DataType]
                              ) extends DynamicTableSink {

  println("table")

  override def getChangelogMode(requestedMode: ChangelogMode): ChangelogMode = {
    ChangelogMode.newBuilder()
      .addContainedKind(RowKind.DELETE)
      .addContainedKind(RowKind.INSERT)
      .addContainedKind(RowKind.UPDATE_AFTER)
      .addContainedKind(RowKind.UPDATE_BEFORE)
      .build()
  }

  override def getSinkRuntimeProvider(context: DynamicTableSink.Context): DynamicTableSink.SinkRuntimeProvider =

    SinkFunctionProvider.of(PhoenixSinkFunction(options,fieldDataTypes))

  override def copy(): DynamicTableSink = PhoenixDynamicTable(options, fieldDataTypes)

  override def asSummaryString(): String = "phoenix dynamic table"
}
