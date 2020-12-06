package com.hiscat.flink.connector.phoenix

import java.util

import com.hiscat.flink.connector.phoenix.PhoenixDynamicTableFactory.{TABLE_NAME, URL}
import org.apache.flink.configuration.{ConfigOption, ConfigOptions}
import org.apache.flink.shaded.guava18.com.google.common.collect.Sets
import org.apache.flink.table.connector.sink.DynamicTableSink
import org.apache.flink.table.factories.{DynamicTableFactory, DynamicTableSinkFactory, FactoryUtil}
import org.apache.flink.table.types.DataType

case class PhoenixDynamicTableFactory() extends DynamicTableSinkFactory {

  println("factory")

  override def createDynamicTableSink(context: DynamicTableFactory.Context): DynamicTableSink = {
    val helper = FactoryUtil.createTableFactoryHelper(this, context)

    helper.validate()

    val options = helper.getOptions
    val url = options.get(URL)
    val tableName = options.get(TABLE_NAME)
    val phoenixOptions = PhoenixOptions(url, tableName)

    val fieldDataTypes: Array[DataType] = context.getCatalogTable.getSchema.getFieldDataTypes

    PhoenixDynamicTable(phoenixOptions, fieldDataTypes)
  }

  override def factoryIdentifier(): String = "phoenix"

  override def requiredOptions(): util.Set[ConfigOption[_]] = Sets.newHashSet(URL, TABLE_NAME)

  override def optionalOptions(): util.Set[ConfigOption[_]] = Sets.newHashSet()
}

object PhoenixDynamicTableFactory {
  private val URL: ConfigOption[String] = ConfigOptions.key("url")
    .stringType()
    .noDefaultValue()
  private val TABLE_NAME: ConfigOption[String] = ConfigOptions.key("table-name")
    .stringType()
    .noDefaultValue()
}
