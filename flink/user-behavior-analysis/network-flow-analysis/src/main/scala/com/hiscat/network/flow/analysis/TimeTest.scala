package com.hiscat.network.flow.analysis

import java.time.{LocalDateTime, ZoneOffset}
import java.time.format.DateTimeFormatter

object TimeTest {
  def main(args: Array[String]): Unit = {
    val time = "18/05/2015:07:05:44"
    val milli = LocalDateTime.parse(time, DateTimeFormatter.ofPattern("dd/MM/yyyy:HH:mm:ss"))
      .toInstant(ZoneOffset.ofHours(8))
      .toEpochMilli
    println(milli)

  }
}
