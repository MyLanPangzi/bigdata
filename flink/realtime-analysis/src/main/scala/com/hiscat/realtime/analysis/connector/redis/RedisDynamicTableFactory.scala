package com.hiscat.realtime.analysis.connector.redis

import java.util

import com.hiscat.realtime.analysis.connector.redis.RedisDynamicTableFactory._
import org.apache.flink.api.common.serialization.{DeserializationSchema, SerializationSchema}
import org.apache.flink.configuration.{ConfigOption, ConfigOptions, ReadableConfig}
import org.apache.flink.formats.json.JsonFormatFactory
import org.apache.flink.shaded.guava18.com.google.common.collect.Sets
import org.apache.flink.table.api.TableSchema
import org.apache.flink.table.connector.format.{DecodingFormat, EncodingFormat}
import org.apache.flink.table.connector.sink.DynamicTableSink
import org.apache.flink.table.connector.source.DynamicTableSource
import org.apache.flink.table.data.RowData
import org.apache.flink.table.factories.{DynamicTableFactory, DynamicTableSinkFactory, DynamicTableSourceFactory, FactoryUtil}

class RedisDynamicTableFactory() extends DynamicTableSinkFactory with DynamicTableSourceFactory {

  override def factoryIdentifier(): String = "redis"

  override def requiredOptions(): util.Set[ConfigOption[_]] = Sets.newHashSet(HOSTS, PORT, MAP_KEY)

  override def optionalOptions(): util.Set[ConfigOption[_]] = Sets.newHashSet(KEY_DELIMITER, FactoryUtil.FORMAT)

  override def createDynamicTableSink(context: DynamicTableFactory.Context): DynamicTableSink = {
    val helper = FactoryUtil.createTableFactoryHelper(this, context)

    helper.validate()

    val encodingFormat: EncodingFormat[SerializationSchema[RowData]] = helper.discoverEncodingFormat(classOf[JsonFormatFactory], FactoryUtil.FORMAT)
    val schema = context.getCatalogTable.getSchema
    RedisDynamicTable(
      getRedisOptions(helper.getOptions),
      encodingFormat,
      null,
      schema.toPhysicalRowDataType,
      getPrimaryKeys(schema)
    )
  }

  override def createDynamicTableSource(context: DynamicTableFactory.Context): DynamicTableSource = {
    val helper = FactoryUtil.createTableFactoryHelper(this, context)
    val decodingFormat: DecodingFormat[DeserializationSchema[RowData]] = helper.discoverDecodingFormat(
      classOf[JsonFormatFactory], FactoryUtil.FORMAT
    )

    val schema = context.getCatalogTable.getSchema
    RedisDynamicTable(
      getRedisOptions(helper.getOptions),
      null,
      decodingFormat,
      schema.toPhysicalRowDataType,
      getPrimaryKeys(schema)
    )
  }

  private def getPrimaryKeys(schema: TableSchema) = {
    import scala.collection.JavaConverters._
    schema.getPrimaryKey.get().getColumns.asScala
  }

  private def getRedisOptions(options: ReadableConfig) =
    RedisOptions(
      options.get(HOSTS),
      options.get(PORT),
      options.get(MAP_KEY),
      options.get(KEY_DELIMITER)
    )
}

object RedisDynamicTableFactory {

  private val HOSTS: ConfigOption[String] = ConfigOptions.key("host")
    .stringType()
    .noDefaultValue()

  private val PORT: ConfigOption[Integer] = ConfigOptions.key("port")
    .intType()
    .defaultValue(6379)

  private val MAP_KEY: ConfigOption[String] = ConfigOptions.key("map-key")
    .stringType()
    .noDefaultValue()

  private val KEY_DELIMITER: ConfigOption[String] = ConfigOptions.key("key-delimiter")
    .stringType()
    .defaultValue("_")


}

