package com.hiscat.flink.table.connector.socket

import java.util

import com.hiscat.flink.table.connector.socket.ChangelogCsvFormatFactory.COLUMN_DELIMITER
import org.apache.flink.api.common.serialization.DeserializationSchema
import org.apache.flink.configuration.{ConfigOption, ConfigOptions, ReadableConfig}
import org.apache.flink.table.connector.format.DecodingFormat
import org.apache.flink.table.data.RowData
import org.apache.flink.table.factories.{DeserializationFormatFactory, DynamicTableFactory, FactoryUtil}

case class ChangelogCsvFormatFactory() extends DeserializationFormatFactory {
  override def createDecodingFormat(context: DynamicTableFactory.Context,
                                    formatOptions: ReadableConfig): DecodingFormat[DeserializationSchema[RowData]] = {
    FactoryUtil.validateFactoryOptions(this, formatOptions)
    val columnDelimiter = formatOptions.get(COLUMN_DELIMITER)
    ChangelogCsvFormat(columnDelimiter)
  }

  override def factoryIdentifier(): String = "changelog-csv"

  override def requiredOptions(): util.Set[ConfigOption[_]] = util.Collections.emptySet()

  override def optionalOptions(): util.Set[ConfigOption[_]] =
    util.Collections.singleton(COLUMN_DELIMITER)
}

object ChangelogCsvFormatFactory {
  private val COLUMN_DELIMITER: ConfigOption[String] = ConfigOptions.key("column-delimiter")
    .stringType()
    .defaultValue("|")


}
