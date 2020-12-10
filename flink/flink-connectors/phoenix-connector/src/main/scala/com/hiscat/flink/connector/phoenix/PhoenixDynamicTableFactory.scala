package com.hiscat.flink.connector.phoenix

import java.util

import com.hiscat.flink.connector.phoenix.PhoenixDynamicTableFactory.{TABLE_NAME, URL}
import org.apache.flink.configuration.{ConfigOption, ConfigOptions}
import org.apache.flink.shaded.guava18.com.google.common.collect.Sets
import org.apache.flink.table.connector.sink.DynamicTableSink
import org.apache.flink.table.connector.source.DynamicTableSource
import org.apache.flink.table.factories.{DynamicTableFactory, DynamicTableSinkFactory, DynamicTableSourceFactory, FactoryUtil}
import org.apache.flink.table.types.DataType

case class PhoenixDynamicTableFactory() extends DynamicTableSinkFactory with DynamicTableSourceFactory {

  override def createDynamicTableSink(context: DynamicTableFactory.Context): DynamicTableSink = {
    val helper = FactoryUtil.createTableFactoryHelper(this, context)

    helper.validate()

    val fieldDataTypes: Array[DataType] = context.getCatalogTable.getSchema.getFieldDataTypes

    PhoenixDynamicTableSink(getPhoenixOptions(helper), fieldDataTypes)
  }

  private def getPhoenixOptions(helper: FactoryUtil.TableFactoryHelper) = {
    val options = helper.getOptions
    val url = options.get(URL)
    val tableName = options.get(TABLE_NAME)
    val phoenixOptions = PhoenixOptions(url, tableName)
    phoenixOptions
  }

  override def factoryIdentifier(): String = "phoenix"

  override def requiredOptions(): util.Set[ConfigOption[_]] = Sets.newHashSet(URL, TABLE_NAME)

  override def optionalOptions(): util.Set[ConfigOption[_]] = Sets.newHashSet()

  override def createDynamicTableSource(context: DynamicTableFactory.Context): DynamicTableSource = {
    val helper = FactoryUtil.createTableFactoryHelper(this, context)

    helper.validate()

    val pks = context.getCatalogTable.getSchema.getPrimaryKey.get().getColumns

    println("source ")
    import scala.collection.JavaConverters._
    PhoenixDynamicLookupTableSource(getPhoenixOptions(helper), pks.asScala.toArray)
  }
}

object PhoenixDynamicTableFactory {
  private val URL: ConfigOption[String] = ConfigOptions.key("url")
    .stringType()
    .noDefaultValue()
  private val TABLE_NAME: ConfigOption[String] = ConfigOptions.key("table-name")
    .stringType()
    .noDefaultValue()
}
