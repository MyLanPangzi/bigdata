package com.hiscat.flink.table.connector.socket

import java.util
import java.util.regex.Pattern

import org.apache.flink.api.common.serialization.DeserializationSchema
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.table.connector.RuntimeConverter.Context
import org.apache.flink.table.connector.source.DynamicTableSource
import org.apache.flink.table.data.RowData
import org.apache.flink.table.types.DataType
import org.apache.flink.table.types.logical.{LogicalType, LogicalTypeRoot}
import org.apache.flink.types.{Row, RowKind}

case class ChangelogCsvDeserializer(converter: DynamicTableSource.DataStructureConverter,
                                    producedDataType: TypeInformation[RowData],
                                    parsingTypes: util.List[LogicalType],
                                    columnDelimiter: String) extends DeserializationSchema[RowData] {

  override def open(context: DeserializationSchema.InitializationContext): Unit = {
    converter.open(Context.create(classOf[ChangelogCsvDeserializer].getClassLoader))
  }

  def parse(root: LogicalTypeRoot, value: String): Any = {
    root match {
      case LogicalTypeRoot.INTEGER => value.toInt
      case LogicalTypeRoot.VARCHAR => value
      case _ => throw new IllegalArgumentException("not support type" + root)
    }
  }

  override def deserialize(message: Array[Byte]): RowData = {
    val columns = new String(message).split(Pattern.quote(columnDelimiter))
    val rowKind = RowKind.valueOf(columns(0))
    val row = new Row(rowKind, parsingTypes.size())
    import scala.collection.JavaConverters._
    parsingTypes.asScala.indices.foreach(i => {
      row.setField(i, parse(parsingTypes.get(i).getTypeRoot, columns(i + 1)))
    })
    converter.toInternal(row).asInstanceOf[RowData]
  }

  override def isEndOfStream(nextElement: RowData): Boolean = false

  override def getProducedType: TypeInformation[RowData] = producedDataType
}
