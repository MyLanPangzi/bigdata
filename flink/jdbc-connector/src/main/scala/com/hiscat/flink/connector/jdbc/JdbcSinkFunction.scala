package com.hiscat.flink.connector.jdbc

import java.sql.{Connection, Driver, DriverManager, PreparedStatement}
import java.util.Properties

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.table.data.RowData
import org.apache.flink.table.types.DataType
import org.apache.flink.util.ChildFirstClassLoader

import scala.util.{Failure, Success, Try}

case class JdbcSinkFunction(
                             options: JdbcConnectionOptions,
                             fieldNames: Array[String],
                             rowDataType: DataType
                           ) extends RichSinkFunction[RowData] {

  println("sink")
  var connection: Connection = _

  //  lazy private val parameters = fieldNames.map(_ => "?").mkString("(", ",", ")")
  //  lazy private val upsertStatement: PreparedStatement = connection.prepareStatement(s"UPSERT INTO ${options.tableName} VALUES $parameters")
  //  lazy private val insertStatement: PreparedStatement = connection.prepareStatement(s"INSERT INTO ${options.tableName} VALUES $parameters")
  //  lazy private val deleteStatement: PreparedStatement = connection.prepareStatement(s"DELETE FROM ${options.tableName} ")

  override def open(parameters: Configuration): Unit = {
    Try {
      println(getClass.getClassLoader)
      println(Thread.currentThread().getContextClassLoader)
      Class.forName("com.mysql.jdbc.Driver")
      //      val driver = Thread.currentThread().getContextClassLoader.loadClass(options.driver)
//      println(driver)
//      connection = if (options.username.isDefined) {
//        DriverManager.getConnection(options.url, options.username.get, options.password.orNull)
//      } else {
//      val driver = this.getClass.getClassLoader.loadClass(options.driver)
//      println(driver)
//      driver.newInstance()
//      import scala.reflect.runtime.ReflectionUtils.
      DriverManager.getConnection(options.url)
      //      }
//      connection.setAutoCommit(true)
      println(connection)
    } match {
      case Failure(exception) =>
        println("exception")
        exception.printStackTrace()
      case Success(value) => println(value)
    }
    println("open")
  }

  override def invoke(value: RowData, context: SinkFunction.Context[_]): Unit = {
    println("invoke ")
    //    import scala.collection.JavaConverters._
    //    rowDataType.getChildren.asScala.zipWithIndex
    //      .map(e => RowData.createFieldGetter(e._1.getLogicalType, e._2))
    //      .map(g => g.getFieldOrNull(value))
    //      .map {
    //        case e: StringData => new String(e.toBytes)
    //        case e: DecimalData => e.toBigDecimal
    //        case e: TimestampData => new Timestamp(e.getMillisecond)
    //        case e => e
    //      }.zipWithIndex
    //      .foreach {
    //        case (value, i) =>
    //          if (options.enableUpsert)
    //            upsertStatement.setObject(i + 1, value)
    //          else
    //            insertStatement.setObject(i + 1, value)
    //      }
    //    if (options.enableUpsert) {
    //      upsertStatement.execute()
    //    } else {
    //      insertStatement.execute()
    //    }
  }

  override def close(): Unit = {
    println("close")
    //    deleteStatement.close()
    //    upsertStatement.close()
    //    insertStatement.close()
    //    connection.close()
  }
}
