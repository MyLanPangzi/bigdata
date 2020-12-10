package com.hiscat.flink.connector.phoenix

import java.sql.{Connection, PreparedStatement}
import java.util.Properties

import org.apache.flink.table.data.{GenericRowData, RowData, StringData}
import org.apache.flink.table.functions.TableFunction
import org.apache.flink.table.types.DataType
import org.apache.phoenix.jdbc.PhoenixDriver

import scala.annotation.varargs

case class PhoenixLookupTableFunction(
                                       phoenixOptions: PhoenixOptions,
                                       pks: Array[String]
                                     ) extends TableFunction[RowData] {

  lazy private val connection: Connection = new PhoenixDriver().connect(phoenixOptions.url, new Properties())
  lazy private val condition: String = pks.map(e => s"$e = ?").mkString(" AND ")
  lazy private val st: PreparedStatement = connection.prepareStatement(s"SELECT * FROM ${phoenixOptions.tableName} WHERE 1=1 AND $condition ")

  @varargs
  def eval(keys: AnyRef*): Unit = {
    println("eval")
    keys.zipWithIndex.foreach {
      case (value, i) => st.setObject(i + 1, value)
    }

    val rs = st.executeQuery()
    while (rs.next()) {
      val data = GenericRowData.of((0 until rs.getMetaData.getColumnCount)
        .map(i => {
          val value = rs.getObject(i + 1)
          value match {
            case _: String => StringData.fromString(value.toString)
            case _ => value
          }
        }): _*)
      collect(data)
    }
    rs.close()
    st.clearParameters()
  }

  override def close(): Unit = {
    st.close()
    connection.close()
  }
}
