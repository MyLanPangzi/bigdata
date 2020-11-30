package com.hiscat.flink.connector.jdbc

import java.util

import com.hiscat.flink.connector.jdbc.JdbcExtendedDynamicTableFactory._
import org.apache.flink.configuration.{ConfigOption, ConfigOptions}
import org.apache.flink.shaded.guava18.com.google.common.collect.Sets
import org.apache.flink.table.connector.sink.DynamicTableSink
import org.apache.flink.table.connector.source.DynamicTableSource
import org.apache.flink.table.factories.{DynamicTableFactory, DynamicTableSinkFactory, DynamicTableSourceFactory, FactoryUtil}

case class JdbcExtendedDynamicTableFactory() extends DynamicTableSinkFactory with DynamicTableSourceFactory {
  override def factoryIdentifier(): String = "jdbc-extended"

  override def requiredOptions(): util.Set[ConfigOption[_]] = Sets.newHashSet(URL, DRIVER, TABLE_NAME)

  override def optionalOptions(): util.Set[ConfigOption[_]] = Sets.newHashSet(USERNAME, PASSWORD, ENABLE_UPSERT)

  override def createDynamicTableSink(context: DynamicTableFactory.Context): DynamicTableSink = {
    val helper = FactoryUtil.createTableFactoryHelper(this, context)

    helper.validate()

    val options = helper.getOptions
    val jdbcConnectionOptions = JdbcConnectionOptions(
      options.get(URL),
      options.get(DRIVER),
      options.get(TABLE_NAME),
      options.get(ENABLE_UPSERT),
      Option(options.get(USERNAME)),
      Option(options.get(PASSWORD))
    )
    val table = context.getCatalogTable
    val rowDataType = table.getSchema.toPhysicalRowDataType
    val fieldNames = table.getSchema.getFieldNames
    println("factory")
    JdbcExtendedDynamicTable(jdbcConnectionOptions, fieldNames, rowDataType)
  }

  override def createDynamicTableSource(context: DynamicTableFactory.Context): DynamicTableSource = null
}

object JdbcExtendedDynamicTableFactory {
  private val URL: ConfigOption[String] = ConfigOptions.key("url")
    .stringType()
    .noDefaultValue()
  private val DRIVER: ConfigOption[String] = ConfigOptions.key("driver")
    .stringType()
    .noDefaultValue()
  private val TABLE_NAME: ConfigOption[String] = ConfigOptions.key("table-name")
    .stringType()
    .noDefaultValue()
  private val USERNAME: ConfigOption[String] = ConfigOptions.key("username")
    .stringType()
    .noDefaultValue()

  private val PASSWORD: ConfigOption[String] = ConfigOptions.key("password")
    .stringType()
    .noDefaultValue()

  private val ENABLE_UPSERT: ConfigOption[java.lang.Boolean] = ConfigOptions.key("enable-upsert")
    .booleanType()
    .defaultValue(false)

}
