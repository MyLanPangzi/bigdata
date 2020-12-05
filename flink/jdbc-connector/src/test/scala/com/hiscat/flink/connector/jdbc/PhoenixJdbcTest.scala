package com.hiscat.flink.connector.jdbc

import java.sql.DriverManager

//import org.apache.phoenix.jdbc.PhoenixDriver
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite

class PhoenixJdbcTest extends AnyFunSuite with BeforeAndAfter {

  test("phoenix write") {
//    Class.forName(classOf[PhoenixDriver].getName)
    val connection = DriverManager.getConnection("jdbc:phoenix:hadoop102:2181")
    connection.setAutoCommit(true)
    //    val statement = connection.createStatement()
    //    statement.execute("upsert into student values(1,'hello')")
    //    statement.close()

    val preparedStatement = connection.prepareStatement("upsert into student values(?,?)")
    preparedStatement.setLong(1, 1)
    preparedStatement.setString(2, "world")
    preparedStatement.execute()
    preparedStatement.close()

    connection.close()
  }


}
