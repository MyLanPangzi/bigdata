package com.hiscat.realtime.analysis.connector

import org.apache.flink.api.common.serialization.SerializationSchema
import org.apache.flink.table.connector.ChangelogMode
import org.apache.flink.table.connector.format.EncodingFormat
import org.apache.flink.table.connector.sink.{DynamicTableSink, SinkFunctionProvider}
import org.apache.flink.table.connector.sink.abilities.SupportsPartitioning
import org.apache.flink.table.data.RowData
import org.apache.flink.table.data.RowData.FieldGetter
import org.apache.flink.table.types.DataType
import org.apache.flink.types.RowKind

import java.util
import scala.collection.mutable

case class HudiDynamicTableSink(
                                 hudiTableOptions: HudiTableOptions,
                                 encoder: EncodingFormat[SerializationSchema[RowData]],
                                 consumeDataType: DataType,
                                 pkGetters: mutable.Seq[FieldGetter],
                                 partitionGetters: mutable.Seq[FieldGetter],
                                 preCombineKeyGetter: FieldGetter,
                               ) extends DynamicTableSink with SupportsPartitioning {
  override def getChangelogMode(changelogMode: ChangelogMode): ChangelogMode =
    ChangelogMode.newBuilder()
      .addContainedKind(RowKind.INSERT)
      .addContainedKind(RowKind.DELETE)
      .addContainedKind(RowKind.UPDATE_BEFORE)
      .addContainedKind(RowKind.UPDATE_AFTER)
      .build()

  override def getSinkRuntimeProvider(context: DynamicTableSink.Context): DynamicTableSink.SinkRuntimeProvider = {
    println("hudi getSinkRuntimeProvider")
    val serializer: SerializationSchema[RowData] = encoder.createRuntimeEncoder(context, consumeDataType)
    SinkFunctionProvider.of(
      HudiDynamicTableSinkFunction(
        hudiTableOptions,
        serializer,
        pkGetters,
        partitionGetters,
        preCombineKeyGetter
      )
    )
  }

  override def copy(): DynamicTableSink = {

    null
  }

  override def asSummaryString(): String = "hudi dynamic table sink"

  override def applyStaticPartition(map: util.Map[String, String]): Unit = {
    println(map)
  }

}
