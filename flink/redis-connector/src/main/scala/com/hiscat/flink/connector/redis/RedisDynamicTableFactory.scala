package com.hiscat.flink.connector.redis

import java.util

import com.hiscat.flink.connector.redis.RedisDynamicTableFactory._
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
import org.apache.flink.table.types.DataType

import scala.collection.JavaConverters._
import scala.collection.mutable


class RedisDynamicTableFactory() extends DynamicTableSinkFactory with DynamicTableSourceFactory {

  override def factoryIdentifier(): String = "redis"

  override def requiredOptions(): util.Set[ConfigOption[_]] = Sets.newHashSet(HOSTNAME, PORT, HASH_KEY)

  override def optionalOptions(): util.Set[ConfigOption[_]] = Sets.newHashSet(KEY_DELIMITER, TTL, FactoryUtil.FORMAT)

  override def createDynamicTableSink(context: DynamicTableFactory.Context): DynamicTableSink = {
    val helper = FactoryUtil.createTableFactoryHelper(this, context)

    helper.validate()
    val encodingFormat: EncodingFormat[SerializationSchema[RowData]] = helper.discoverEncodingFormat(classOf[JsonFormatFactory], FactoryUtil.FORMAT)

    val options = helper.getOptions
    val redisOptions = RedisOptions(
      options.get(HOSTNAME),
      options.get(PORT),
      options.get(HASH_KEY),
      options.get(KEY_DELIMITER),
      options.get(TTL)
    )
    println("createDynamicTableSink")
    println(redisOptions)
    val schema = context.getCatalogTable.getSchema
    val fieldNames = schema.getFieldNames
    val pks: mutable.Seq[Int] = schema.getPrimaryKey.get().getColumns.asScala.map(fieldNames.indexOf)
    val pkTypes: mutable.Seq[DataType] = pks.map(schema.getFieldDataType(_).get())

    RedisDynamicTable(
      redisOptions,
      encodingFormat,
      null,
      schema.toPhysicalRowDataType,
      pks,
      pkTypes
    )
  }

  override def createDynamicTableSource(context: DynamicTableFactory.Context): DynamicTableSource = {
    println("createDynamicTableSource")
    val helper = FactoryUtil.createTableFactoryHelper(this, context)
    helper.validate()
    val decodingFormat: DecodingFormat[DeserializationSchema[RowData]] = helper.discoverDecodingFormat(
      classOf[JsonFormatFactory], FactoryUtil.FORMAT
    )
    val options = helper.getOptions
    val redisOptions = RedisOptions(
      options.get(HOSTNAME),
      options.get(PORT),
      options.get(HASH_KEY),
      options.get(KEY_DELIMITER),
      options.get(TTL)
    )
    println("createDynamicTableSink")
    println(redisOptions)
    val schema = context.getCatalogTable.getSchema
    val fieldNames = schema.getFieldNames
    val pks: mutable.Seq[Int] = schema.getPrimaryKey.get().getColumns.asScala.map(fieldNames.indexOf)
    val pkTypes: mutable.Seq[DataType] = pks.map(schema.getFieldDataType(_).get())

    RedisDynamicTable(
      redisOptions,
      null,
      decodingFormat,
      schema.toPhysicalRowDataType,
      pks,
      pkTypes
    )
  }

  private def getPrimaryKeys(schema: TableSchema) = {
    import scala.collection.JavaConverters._
    schema.getPrimaryKey.get().getColumns.asScala
  }

  //  private def getRedisOptions(options: ReadableConfig) =
  //    RedisOptions(
  //      options.get(HOSTNAME),
  //      options.get(PORT),
  //      options.get(HASH_KEY),
  //      options.get(KEY_DELIMITER)
  //    )
}

object RedisDynamicTableFactory {

  private val HOSTNAME: ConfigOption[String] = ConfigOptions.key("hostname")
    .stringType()
    .noDefaultValue()

  private val PORT: ConfigOption[Integer] = ConfigOptions.key("port")
    .intType()
    .defaultValue(6379)

  private val HASH_KEY: ConfigOption[String] = ConfigOptions.key("hash-key")
    .stringType()
    .noDefaultValue()

  private val KEY_DELIMITER: ConfigOption[String] = ConfigOptions.key("key-delimiter")
    .stringType()
    .defaultValue("_")

  private val TTL: ConfigOption[java.lang.Long] = ConfigOptions.key("ttl")
    .longType()
    .defaultValue(-1)

}
