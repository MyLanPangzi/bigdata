package com.hiscat.flink.connector.redis

import org.apache.flink.api.common.serialization.{DeserializationSchema, SerializationSchema}
import org.apache.flink.table.connector.ChangelogMode
import org.apache.flink.table.connector.format.{DecodingFormat, EncodingFormat}
import org.apache.flink.table.connector.sink.{DynamicTableSink, SinkFunctionProvider}
import org.apache.flink.table.connector.source.{LookupTableSource, TableFunctionProvider}
import org.apache.flink.table.data.RowData
import org.apache.flink.table.types.DataType
import org.apache.flink.types.RowKind

import scala.collection.mutable

case class RedisDynamicTable(
                              redisOptions: RedisOptions,
                              encodingFormat: EncodingFormat[SerializationSchema[RowData]],
                              decodingFormat: DecodingFormat[DeserializationSchema[RowData]],
                              consumedDataTypes: DataType,
                              pks: mutable.Seq[Int],
                              pkTypes: mutable.Seq[DataType]
                            ) extends DynamicTableSink with LookupTableSource {
  override def getChangelogMode(requestedMode: ChangelogMode): ChangelogMode =
    ChangelogMode.newBuilder()
      .addContainedKind(RowKind.INSERT)
      .addContainedKind(RowKind.DELETE)
      .addContainedKind(RowKind.UPDATE_AFTER)
      .build()

  override def getSinkRuntimeProvider(context: DynamicTableSink.Context): DynamicTableSink.SinkRuntimeProvider = {
    println("getSinkRuntimeProvider")
    val serializer = encodingFormat.createRuntimeEncoder(context, consumedDataTypes)
    SinkFunctionProvider.of(RedisSinkFunction(redisOptions, serializer, pks, pkTypes))
  }

  override def getLookupRuntimeProvider(context: LookupTableSource.LookupContext): LookupTableSource.LookupRuntimeProvider = {
    val deserializer = decodingFormat.createRuntimeDecoder(context, consumedDataTypes)
    TableFunctionProvider.of(RedisTableFunction(redisOptions, deserializer))
  }

  override def copy(): RedisDynamicTable = RedisDynamicTable(
    redisOptions,
    encodingFormat,
    decodingFormat,
    consumedDataTypes,
    pks,
    pkTypes
  )

  override def asSummaryString(): String = "redis dynamic table"
}
