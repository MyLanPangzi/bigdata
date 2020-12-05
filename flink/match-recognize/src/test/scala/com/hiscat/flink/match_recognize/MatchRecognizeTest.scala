package com.hiscat.flink.match_recognize

import java.time.{LocalDateTime, ZoneId, ZoneOffset}
import java.time.format.DateTimeFormatter
import java.util.Locale

import org.apache.flink.streaming.api._
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.api._
import org.apache.flink.types.Row
import org.scalatest.{BeforeAndAfter, BeforeAndAfterEach}
import org.scalatest.funsuite.AnyFunSuite

class MatchRecognizeTest extends AnyFunSuite with BeforeAndAfter with BeforeAndAfterEach {
  val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
  val tEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)
  env.setParallelism(1)
  env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
  tEnv.getConfig.setLocalTimeZone(ZoneOffset.ofHours(8))
  val formatter: DateTimeFormatter = DateTimeFormatter.ofPattern("dd-MMM-yy HH:mm:ss").withLocale(Locale.UK)

  test("example") {
    val data =
      """
        |'ACME'  '01-Apr-11 10:00:00'   12      1
        |'ACME'  '01-Apr-11 10:00:01'   17      2
        |'ACME'  '01-Apr-11 10:00:02'   19      1
        |'ACME'  '01-Apr-11 10:00:03'   21      3
        |'ACME'  '01-Apr-11 10:00:04'   25      2
        |'ACME'  '01-Apr-11 10:00:05'   18      1
        |'ACME'  '01-Apr-11 10:00:06'   15      1
        |'ACME'  '01-Apr-11 10:00:07'   14      2
        |'ACME'  '01-Apr-11 10:00:08'   24      2
        |'ACME'  '01-Apr-11 10:00:09'   25      2
        |'ACME'  '01-Apr-11 10:00:10'   19      1
        |""".stripMargin
    registerStockDdMMMyyHHmmss(data)
    val table = tEnv.sqlQuery(
      """
        |SELECT *
        |FROM stock
        |    MATCH_RECOGNIZE (
        |        PARTITION BY symbol
        |        ORDER BY rowtime
        |        MEASURES
        |            START_ROW.rowtime AS start_tstamp,
        |            LAST(PRICE_DOWN.rowtime) AS bottom_tstamp,
        |            LAST(PRICE_UP.rowtime) AS end_tstamp
        |        ONE ROW PER MATCH
        |        AFTER MATCH SKIP TO LAST PRICE_UP
        |        PATTERN (START_ROW PRICE_DOWN+ PRICE_UP)
        |        DEFINE
        |            PRICE_DOWN AS
        |                (LAST(PRICE_DOWN.price, 1) IS NULL AND PRICE_DOWN.price < START_ROW.price) OR
        |                    PRICE_DOWN.price < LAST(PRICE_DOWN.price, 1),
        |            PRICE_UP AS
        |                PRICE_UP.price > LAST(PRICE_DOWN.price, 1)
        |    ) MR
        |""".stripMargin)
    table.toAppendStream[Row]
      .print()
  }

  test("aggregate"){
    val data =
      """
        |'ACME'  '01-Apr-11 10:00:00'   12      1
        |'ACME'  '01-Apr-11 10:00:01'   17      2
        |'ACME'  '01-Apr-11 10:00:02'   13      1
        |'ACME'  '01-Apr-11 10:00:03'   16      3
        |'ACME'  '01-Apr-11 10:00:04'   25      2
        |'ACME'  '01-Apr-11 10:00:05'   2       1
        |'ACME'  '01-Apr-11 10:00:06'   4       1
        |'ACME'  '01-Apr-11 10:00:07'   10      2
        |'ACME'  '01-Apr-11 10:00:08'   15      2
        |'ACME'  '01-Apr-11 10:00:09'   25      2
        |'ACME'  '01-Apr-11 10:00:10'   25      1
        |'ACME'  '01-Apr-11 10:00:11'   30      1
        |""".stripMargin
    registerStockDdMMMyyHHmmss(data)
    val table = tEnv.sqlQuery(
      """
        |SELECT *
        |FROM stock
        |    MATCH_RECOGNIZE (
        |        PARTITION BY symbol
        |        ORDER BY rowtime
        |        MEASURES
        |            FIRST(A.rowtime) AS start_tstamp,
        |            LAST(A.rowtime) AS end_tstamp,
        |            AVG(A.price) AS avgPrice
        |        ONE ROW PER MATCH
        |        AFTER MATCH SKIP PAST LAST ROW
        |        PATTERN (A+ B)
        |        DEFINE
        |            A AS AVG(A.price) < 15
        |    ) MR
        |""".stripMargin)
    table.toAppendStream[Row]
      .print()
  }

  test("time constraint") {
    val data =
      """
        |'ACME'  '01-Apr-11 10:00:00'   20      1
        |'ACME'  '01-Apr-11 10:20:00'   17      2
        |'ACME'  '01-Apr-11 10:40:00'   18      1
        |'ACME'  '01-Apr-11 11:00:00'   11      3
        |'ACME'  '01-Apr-11 11:20:00'   14      2
        |'ACME'  '01-Apr-11 11:40:00'   9       1
        |'ACME'  '01-Apr-11 12:00:00'   15      1
        |'ACME'  '01-Apr-11 12:20:00'   14      2
        |'ACME'  '01-Apr-11 12:40:00'   24      2
        |'ACME'  '01-Apr-11 13:00:00'   1       2
        |'ACME'  '01-Apr-11 13:20:00'   19      1
        |""".stripMargin

    registerStockDdMMMyyHHmmss(data)

    val table = tEnv.sqlQuery(
      """
        |SELECT *
        |FROM stock
        |    MATCH_RECOGNIZE(
        |        PARTITION BY symbol
        |        ORDER BY rowtime
        |        MEASURES
        |            C.rowtime AS dropTime,
        |            A.price - C.price AS dropDiff
        |      --  ONE ROW PER MATCH
        |       -- AFTER MATCH SKIP PAST LAST ROW
        |        PATTERN (A B* C) WITHIN INTERVAL '1' HOUR
        |        DEFINE
        |            B AS B.price > A.price - 10,
        |            C AS C.price < A.price - 10
        |    )
        |""".stripMargin)
    table.printSchema()
    table.toAppendStream[Row]
      .print()
    //    val table = tEnv.sqlQuery(
    //      """
    //        |SELECT symbol, TIMESTAMPADD(HOUR, 8, rowtime) rowtime, price, tax
    //        |FROM stock
    //        |""".stripMargin)
    //    table.printSchema()
    //    table.toAppendStream[Row]
    //      .print()
  }

  private def registerStockDdMMMyyHHmmss(data: String): Unit = {
    val input = env.fromCollection(parseStocks(data))
      .assignAscendingTimestamps(_.rowtime)
    tEnv.createTemporaryView("stock", input, $"symbol", $"rowtime".rowtime, $"price", $"tax")
  }

  private def parseStocks(data: String): Array[Stock] = {
    data.split("\n")
      .filter(_.trim.nonEmpty)
      .map(e => {
        val split = e.split("\\s{2,}").map(_.trim)
        val time = LocalDateTime.parse(split(1).replaceAll("'", ""), formatter)
        val ts = time.toEpochSecond(ZoneOffset.ofHours(8)) * 1000
        Stock(split(0).replaceAll("'", ""), ts, split(2).toLong, split(3).toInt)
      })
  }

  test("output mode") {
    val data =
      """
        | XYZ      1     10       2018-09-17 10:00:02
        | XYZ      2     12       2018-09-17 10:00:03
        | XYZ      1     13       2018-09-17 10:00:04
        | XYZ      2     11       2018-09-17 10:00:05
        |""".stripMargin
    registerStockTable(data)
    val table = tEnv.sqlQuery(
      """
        |SELECT *
        |FROM stocks
        |    MATCH_RECOGNIZE(
        |        PARTITION BY symbol
        |        ORDER BY rowtime
        |        MEASURES
        |            FIRST(A.price) AS startPrice,
        |            LAST(A.price) AS topPrice,
        |            B.price AS lastPrice
        |     --   ONE ROW PER MATCH
        |        AFTER MATCH SKIP PAST LAST ROW
        |        PATTERN (A+ B)
        |        DEFINE
        |            A AS LAST(A.price, 1) IS NULL OR A.price > LAST(A.price, 1),
        |            B AS B.price < LAST(A.price)
        |    )
        |""".stripMargin)
    //    table.printSchema()
    table.toAppendStream[Row]
      .print()
  }


  test("after match strategy") {
    val data =
      """
        | XYZ      1     7       2018-09-17 10:00:01
        | XYZ      2     9       2018-09-17 10:00:02
        | XYZ      1     10      2018-09-17 10:00:03
        | XYZ      2     5       2018-09-17 10:00:04
        | XYZ      2     10      2018-09-17 10:00:05
        | XYZ      2     7       2018-09-17 10:00:06
        | XYZ      2     14      2018-09-17 10:00:07
        |""".stripMargin
    registerStockTable(data)
    val table = tEnv.sqlQuery(
      """
        |SELECT *
        |FROM stocks
        |    MATCH_RECOGNIZE(
        |        PARTITION BY symbol
        |        ORDER BY rowtime
        |        MEASURES
        |            SUM(A.price) AS sumPrice,
        |            FIRST(rowtime) AS startTime,
        |            LAST(rowtime) AS endTime
        |      --  AFTER MATCH SKIP PAST LAST ROW
        |      --  AFTER MATCH SKIP TO NEXT ROW
        |      --  AFTER MATCH SKIP TO LAST A
        |      --  AFTER MATCH SKIP TO FIRST A
        |        PATTERN (A+ C)
        |        DEFINE
        |            A AS SUM(A.price) < 30
        |    )
        |""".stripMargin)
    table.toAppendStream[Row]
      .print()

  }

  private def registerStockTable(data: String): Unit = {
    val stocks = parseStockYyyyMMddHHmmss(data)
    val input = env.fromCollection(stocks).assignAscendingTimestamps(_.rowtime * 1000)
    tEnv.createTemporaryView("stocks", input, $"symbol", $"tax", $"price", $"rowtime".rowtime)
  }

  private def parseStockYyyyMMddHHmmss(data: String): Array[Stock] = {
    val stocks = data.split("\n")
      .filter(_.trim.nonEmpty)
      .map(e => {
        val split = e.split("\\s{2,}").map(_.trim)
        Stock(
          split(0),
          LocalDateTime.parse(split(3), DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")).toEpochSecond(ZoneOffset.ofHours(8)),
          split(2).toLong,
          split(1).toInt
        )
      })
    stocks
  }

  override protected def afterEach(): Unit = {
    env.fromElements(1).print()
    env.execute("test")
  }
}
