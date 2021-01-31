package com.hiscat.hudi

import org.apache.spark.sql.SparkSession

object ReadTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("test")
      .getOrCreate()
    import spark.implicits._
    spark.read
      .format("hudi")
      .load("hdfs://hdp10:8020/student/*/*/*/*")
      .show()

    spark.close()
  }
}
