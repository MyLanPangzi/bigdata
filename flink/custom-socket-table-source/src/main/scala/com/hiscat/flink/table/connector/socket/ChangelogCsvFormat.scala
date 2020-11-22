package com.hiscat.flink.table.connector.socket

import org.apache.flink.api.common.serialization.DeserializationSchema
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.table.connector.ChangelogMode
import org.apache.flink.table.connector.format.DecodingFormat
import org.apache.flink.table.connector.source.DynamicTableSource
import org.apache.flink.table.data.RowData
import org.apache.flink.table.types.DataType
import org.apache.flink.types.RowKind

case class ChangelogCsvFormat(columnDelimiter: String) extends DecodingFormat[DeserializationSchema[RowData]] {
  override def createRuntimeDecoder(context: DynamicTableSource.Context,
                                    producedDataType: DataType): DeserializationSchema[RowData] = {
    val producedTypeInfo = context.createTypeInformation(producedDataType).asInstanceOf[TypeInformation[RowData]]
    val converter = context.createDataStructureConverter(producedDataType)
    val parsingTypes = producedDataType.getLogicalType.getChildren
    ChangelogCsvDeserializer(converter, producedTypeInfo, parsingTypes, columnDelimiter)
  }

  override def getChangelogMode: ChangelogMode = ChangelogMode.newBuilder()
    .addContainedKind(RowKind.DELETE)
    .addContainedKind(RowKind.INSERT)
    .build()
}
