package com.hiscat.flink.sql.parser

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import com.hiscat.flink.sql.parser._


class SqlCommandParserTest extends AnyFlatSpec with Matchers {

  "sql parser" should "parse sql " in {
    val commands = SqlCommandParser.getCommands(getClass.getResource("/test.sql").toURI.getPath)
    commands.exists(_.isInstanceOf[DmlCommand]) should be (true)
    commands.exists(_.isInstanceOf[DqlCommand]) should be (true)
    commands.exists(_.isInstanceOf[SetCommand]) should be (true)
    commands.exists(_.isInstanceOf[DdlCommand]) should be (true)
  }

}
