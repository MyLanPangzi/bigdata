package com.hiscat.realtime.analysis.connector

import com.google.common.collect.Sets
import com.hiscat.realtime.analysis.connector.HudiDynamicTableFactory.{BASE_PATH, PRECOMBINE_FIELD_PROP, SCHEMA, TABLE_NAME}
import org.apache.flink.api.common.serialization.SerializationSchema
import org.apache.flink.configuration.{ConfigOption, ConfigOptions}
import org.apache.flink.formats.json.JsonFormatFactory
import org.apache.flink.table.connector.format.EncodingFormat
import org.apache.flink.table.connector.sink.DynamicTableSink
import org.apache.flink.table.data.RowData
import org.apache.flink.table.data.RowData.FieldGetter
import org.apache.flink.table.factories.{DynamicTableFactory, DynamicTableSinkFactory, FactoryUtil}

import java.util
import scala.collection.mutable

class HudiDynamicTableFactory extends DynamicTableSinkFactory {
  override def createDynamicTableSink(context: DynamicTableFactory.Context): DynamicTableSink = {
    println("hudi createDynamicTableSink")
    val helper = FactoryUtil.createTableFactoryHelper(this, context)

    helper.validate()

    val options = helper.getOptions
    val basePath = options.get(BASE_PATH)
    val tableName = options.get(TABLE_NAME)
    val preCombineKey = options.get(PRECOMBINE_FIELD_PROP)
    val hudiSchema = options.get(SCHEMA)
    val hudiTableOptions = HudiTableOptions(basePath, tableName, preCombineKey, hudiSchema)

    val encoder: EncodingFormat[SerializationSchema[RowData]] =
      helper.discoverEncodingFormat(classOf[JsonFormatFactory], FactoryUtil.FORMAT)

    import scala.collection.JavaConverters._
    val table = context.getCatalogTable

    val schema = table.getSchema
    val fieldNames = schema.getFieldNames
    val fieldDataTypes = schema.getFieldDataTypes

    val partitionKeys = table.getPartitionKeys.asScala
    val partitionGetters: mutable.Seq[FieldGetter] = partitionKeys.map(e => {
      val index = fieldNames.indexOf(e)
      RowData.createFieldGetter(
        fieldDataTypes(index).getLogicalType,
        index
      )
    })

    val pkGetters = schema.getPrimaryKey.get().getColumns
      .asScala.map(e => {
      val index = fieldNames.indexOf(e)
      RowData.createFieldGetter(
        fieldDataTypes(index).getLogicalType,
        index
      )
    })

    val preCombineKeyPos = fieldNames.indexOf(preCombineKey)
    val preCombineKeyGetter = RowData.createFieldGetter(fieldDataTypes(preCombineKeyPos).getLogicalType, preCombineKeyPos)

    //    val hudiTableSchemaCols = fieldNames.filterNot(partitionKeys.contains)
    //    val schemaMap = hudiTableSchemaCols
    //      .map(e => (e, fieldDataTypes(fieldNames.indexOf(e)).getLogicalType))
    //      .map(kv => {
    //        kv._2 match {
    //          case _: TimestampType => (kv._1, "LONG")
    //          case _ => (kv._1, kv._2.toString)
    //        }
    //      })
    //      .toMap
    //    val schemaStr = JSON.toJSONString(schemaMap, SerializerFeature.NotWriteRootClassName)
    //    println(s"schema ${schemaStr}")

    //partition getters
    //pk getters

    HudiDynamicTableSink(
      hudiTableOptions,
      encoder,
      schema.toPhysicalRowDataType,
      pkGetters,
      partitionGetters,
      preCombineKeyGetter
    )
  }

  override def factoryIdentifier(): String = "hudi"

  override def requiredOptions(): util.Set[ConfigOption[_]] =
    Sets.newHashSet(BASE_PATH, TABLE_NAME, PRECOMBINE_FIELD_PROP, SCHEMA, FactoryUtil.FORMAT)

  override def optionalOptions(): util.Set[ConfigOption[_]] = util.Collections.emptySet()
}

object HudiDynamicTableFactory {
  //  'base-path' = '/test',
  //  'table-name' = 'test',
  //  'precombine-field-prop' = 'ts'
  private val BASE_PATH: ConfigOption[String] = ConfigOptions.key("base-path")
    .stringType()
    .noDefaultValue()

  private val TABLE_NAME: ConfigOption[String] = ConfigOptions.key("table-name")
    .stringType()
    .noDefaultValue()

  private val PRECOMBINE_FIELD_PROP: ConfigOption[String] = ConfigOptions.key("precombine-field-prop")
    .stringType()
    .noDefaultValue()

  private val SCHEMA: ConfigOption[String] = ConfigOptions.key("schema")
    .stringType()
    .noDefaultValue()
}
