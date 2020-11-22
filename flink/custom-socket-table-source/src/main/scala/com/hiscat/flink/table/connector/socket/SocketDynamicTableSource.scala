package com.hiscat.flink.table.connector.socket

import org.apache.flink.api.common.serialization.DeserializationSchema
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.table.connector.ChangelogMode
import org.apache.flink.table.connector.format.DecodingFormat
import org.apache.flink.table.connector.source.{DynamicTableSource, ScanTableSource, SourceFunctionProvider}
import org.apache.flink.table.data.RowData
import org.apache.flink.table.types.DataType

case class SocketDynamicTableSource(hostname: String,
                                    port: Integer,
                                    byteDelimiter: Byte,
                                    decodingFormat: DecodingFormat[DeserializationSchema[RowData]],
                                    producedDataType: DataType) extends ScanTableSource {
  override def copy(): DynamicTableSource = SocketDynamicTableSource(hostname, port, byteDelimiter, decodingFormat, producedDataType)

  override def asSummaryString(): String = "socket table source"

  override def getChangelogMode: ChangelogMode = decodingFormat.getChangelogMode


  override def getScanRuntimeProvider(runtimeProviderContext: ScanTableSource.ScanContext): ScanTableSource.ScanRuntimeProvider = {
    val deserializer = decodingFormat.createRuntimeDecoder(runtimeProviderContext, producedDataType)

    val sourceFunction: SourceFunction[RowData] = SocketSourceFunction(hostname, port, byteDelimiter, deserializer)
    SourceFunctionProvider.of(sourceFunction, false)
  }
}
