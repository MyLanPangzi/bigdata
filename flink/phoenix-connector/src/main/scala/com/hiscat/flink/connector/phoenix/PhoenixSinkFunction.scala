package com.hiscat.flink.connector.phoenix

import java.sql.{Connection, PreparedStatement}
import java.util.Properties

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.table.data.{DecimalData, RowData, StringData}
import org.apache.flink.table.types.DataType
import org.apache.flink.types.RowKind
import org.apache.phoenix.jdbc.PhoenixDriver

case class PhoenixSinkFunction(
                                options: PhoenixOptions,
                                fieldDataTypes: Array[DataType]
                              ) extends RichSinkFunction[RowData] {
  println("sink function")
  lazy private val connection: Connection = new PhoenixDriver().connect(options.url, new Properties())
  lazy private val upsertStatement: PreparedStatement = connection.prepareStatement(s"upsert into ${options.tableName} values(?,?)")

  override def open(parameters: Configuration): Unit = {
    connection.setAutoCommit(true)
  }

  override def invoke(value: RowData, context: SinkFunction.Context[_]): Unit = {
    println("invoke : " + value)
    value.getRowKind match {
      case RowKind.INSERT | RowKind.UPDATE_BEFORE | RowKind.UPDATE_AFTER => upsert(value)
      case RowKind.DELETE =>
    }
  }

  private def upsert(value: RowData) = {
    fieldDataTypes.zipWithIndex.map {
      case (dataType, i) => RowData.createFieldGetter(dataType.getLogicalType, i)
    }
      .map(e => e.getFieldOrNull(value))
      .map {
        case e: StringData => new String(e.toBytes)
        case e: DecimalData => e.toBigDecimal
        case e => e
      }.zipWithIndex
      .foreach {
        case (v, i) => upsertStatement.setObject(i + 1, v)
      }
    upsertStatement.execute()
  }

  override def close(): Unit = {
    println("close")
    upsertStatement.close()
    connection.close()
  }
}
