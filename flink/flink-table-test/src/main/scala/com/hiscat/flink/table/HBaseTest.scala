package com.hiscat.flink.table

import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.ConnectionFactory

object HBaseTest {
  def main(args: Array[String]): Unit = {
    val conf = HBaseConfiguration.create()
    val connection = ConnectionFactory.createConnection(conf)
    val table = connection.getTable(TableName.valueOf(""))

    println(table)
  }
}
