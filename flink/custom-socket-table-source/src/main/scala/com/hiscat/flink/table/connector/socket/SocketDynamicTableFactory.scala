package com.hiscat.flink.table.connector.socket

import java.util

import com.hiscat.flink.table.connector.socket.SocketDynamicTableFactory.{BYTE_DELIMITER, HOSTNAME, PORT}
import org.apache.flink.api.common.serialization.DeserializationSchema
import org.apache.flink.configuration.{ConfigOption, ConfigOptions}
import org.apache.flink.table.connector.format.DecodingFormat
import org.apache.flink.table.connector.source.DynamicTableSource
import org.apache.flink.table.data.RowData
import org.apache.flink.table.factories.{DeserializationFormatFactory, DynamicTableFactory, DynamicTableSourceFactory, FactoryUtil}
import org.apache.flink.table.types.DataType

case class SocketDynamicTableFactory() extends DynamicTableSourceFactory {


  override def createDynamicTableSource(context: DynamicTableFactory.Context): DynamicTableSource = {
    val helper = FactoryUtil.createTableFactoryHelper(this, context)

    helper.validate()

    val options = helper.getOptions
    val hostname = options.get(HOSTNAME)
    val port = options.get(PORT)
    val byteDelimiter = options.get(BYTE_DELIMITER).toByte

    val producedDataType = context.getCatalogTable.getSchema.toPhysicalRowDataType

    val decodingFormat = helper
      .discoverDecodingFormat[DeserializationSchema[RowData], DeserializationFormatFactory](
        classOf[DeserializationFormatFactory],
        FactoryUtil.FORMAT
      )

    SocketDynamicTableSource(
      hostname,
      port,
      byteDelimiter,
      decodingFormat,
      producedDataType
    )
  }

  override def factoryIdentifier(): String = "socket"

  override def requiredOptions(): util.Set[ConfigOption[_]] = {
    val set = new util.HashSet[ConfigOption[_]]()
    set.add(HOSTNAME)
    set.add(PORT)
    set.add(BYTE_DELIMITER)
    set
  }

  override def optionalOptions(): util.Set[ConfigOption[_]] =
    util.Collections.singleton(BYTE_DELIMITER)
}

case object SocketDynamicTableFactory {
  private val HOSTNAME: ConfigOption[String] = ConfigOptions.key("hostname")
    .stringType()
    .noDefaultValue()

  private val PORT: ConfigOption[Integer] = ConfigOptions.key("port")
    .intType()
    .noDefaultValue()

  private val BYTE_DELIMITER: ConfigOption[Integer] = ConfigOptions.key("byte-delimiter")
    .intType()
    .defaultValue(10)


}