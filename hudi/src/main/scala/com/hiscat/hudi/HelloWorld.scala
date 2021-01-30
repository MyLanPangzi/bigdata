package com.hiscat.hudi

import org.apache.hudi.QuickstartUtils._

import scala.collection.JavaConversions._
import org.apache.spark.sql.SaveMode._
import org.apache.hudi.DataSourceReadOptions._
import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.common.model.{HoodieRecord, HoodieRecordPayload}
import org.apache.hudi.config.HoodieWriteConfig._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder


object HelloWorld {
  private val HUDI = "hudi"
  private val TIMESTAMP = "ts"

  private val UUID = "uuid"

  private val PARITION_PATH = "partitionpath"

  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdp")
    val spark = SparkSession.builder()
      .appName("test")
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    import spark.implicits._
    val tableName = s"${HUDI}_trips_cow"
    val basePath = s"/${HUDI}_trips_cow/"
    val dataGen = new DataGenerator()
    insert(spark, tableName, basePath, dataGen)
    snapshotQuery(spark, basePath)
    update(spark, tableName, basePath, dataGen)
    snapshotQuery(spark, basePath)
    incrementalQuery(spark, basePath)
    pointTimeQuery(spark, basePath)
    delete(spark, tableName, basePath, dataGen)
    insertOverwriteTable(spark, tableName, basePath, dataGen)
    insertOverwrite(spark, tableName, basePath, dataGen)
  }


  private def insert(spark: SparkSession, tableName: String, basePath: String, dataGen: DataGenerator): Unit = {
    import spark.implicits._
    val inserts = convertToStringList(dataGen.generateInserts(10))
    val df = spark.read.json(spark.sparkContext.parallelize(inserts, 2))
    df.write
      .format(HUDI)
      .options(getQuickstartWriteConfigs)
      .option(PRECOMBINE_FIELD_OPT_KEY, TIMESTAMP)
      .option(RECORDKEY_FIELD_OPT_KEY, UUID)
      .option(PARTITIONPATH_FIELD_OPT_KEY, PARITION_PATH)
      .option(TABLE_NAME, tableName)
      .mode(Overwrite)
      .save(basePath)
  }

  private def snapshotQuery(spark: SparkSession, basePath: String): Unit = {
    createSnapshotView(spark, basePath)
    import spark._
    //    sql("SELECT COUNT(*) FROM hudi_trips_snapshot").show()
    //    sql("select fare, begin_lon, begin_lat, ts from  hudi_trips_snapshot where fare > 20.0").show()
    //    sql("select * from  hudi_trips_snapshot where fare > 20.0").show()
    sql(
      """
        |select
        |  _hoodie_commit_time,
        |  _hoodie_record_key,
        |  _hoodie_partition_path,
        |   rider, driver, fare
        |from hudi_trips_snapshot
        |where fare > 20
        |""".stripMargin
    )
      .show()
  }

  private def createSnapshotView(spark: SparkSession, basePath: String): Unit = {
    spark.read
      .format(HUDI)
      .load(basePath + "/*/*/*/*")
      .createOrReplaceTempView(HUDI + "_trips_snapshot")
  }

  private def update(spark: SparkSession, tableName: String, basePath: String, dataGen: DataGenerator): Unit = {
    val updates = convertToStringList(dataGen.generateUpdates(10))
    val df = spark.read.json(spark.sparkContext.parallelize(updates, 2))
    df.write
      .format(HUDI)
      .options(getQuickstartWriteConfigs)
      .option(PRECOMBINE_FIELD_OPT_KEY, TIMESTAMP)
      .option(RECORDKEY_FIELD_OPT_KEY, UUID)
      .option(PARTITIONPATH_FIELD_OPT_KEY, PARITION_PATH)
      .option(TABLE_NAME, tableName)
      .mode(Append)
      .save(basePath)
  }

  private def incrementalQuery(spark: SparkSession, basePath: String): Unit = {
    import spark.implicits._
    import spark._
    createSnapshotView(spark, basePath)
    val commits = spark
      .sql("select distinct(_hoodie_commit_time) as commitTime from " + HUDI + "_trips_snapshot order by commitTime")
      .map(_.getString(0))
      .take(50)

    //    commits.show()
    val beginTime = commits(commits.length - 2)
    println(beginTime)
    // commit time we are interested in
    //
    //    // incrementally query data
    spark.read
      .format(HUDI)
      .option(QUERY_TYPE_OPT_KEY, QUERY_TYPE_INCREMENTAL_OPT_VAL)
      .option(BEGIN_INSTANTTIME_OPT_KEY, beginTime)
      .load(basePath)
      .createOrReplaceTempView(HUDI + "_trips_incremental")

    sql("select `_hoodie_commit_time`, fare, begin_lon, begin_lat, " + TIMESTAMP + " from " + HUDI + "_trips_incremental where fare > 20.0")
      .show()
  }

  private def pointTimeQuery(spark: SparkSession, basePath: String): Unit = {
    import spark.implicits._
    import spark._

    val commits = spark
      .sql("select distinct(_hoodie_commit_time) as commitTime from  " + HUDI + "_trips_snapshot order by commitTime")
      .map(k => k.getString(0))
      .take(50)
    val beginTime = "000" // Represents all commits > this time.
    val endTime = commits(commits.length - 2) // commit time we are interested in
    println(endTime)

    spark
      .read
      .format(HUDI)
      .option(QUERY_TYPE_OPT_KEY, QUERY_TYPE_INCREMENTAL_OPT_VAL)
      .option(BEGIN_INSTANTTIME_OPT_KEY, beginTime)
      .option(END_INSTANTTIME_OPT_KEY, endTime)
      .load(basePath)
      .createOrReplaceTempView(s"${HUDI}_trips_point_in_time")
    sql(s"select `_hoodie_commit_time`, fare, begin_lon, begin_lat, $TIMESTAMP from ${HUDI}_trips_point_in_time where fare > 20.0").show()
  }

  private def delete(spark: SparkSession, tableName: String, basePath: String, dataGen: DataGenerator): Unit = {
    import spark.implicits._
    import spark._
    createSnapshotView(spark, basePath)
    println("before delete count")
    spark.sql("select count(*) from " + HUDI + "_trips_snapshot").show()

    sql("select " + UUID + ", " + PARITION_PATH + " from " + HUDI + "_trips_snapshot").count()
    // fetch two records to be deleted
    val ds = spark
      .sql("select " + UUID + ", " + PARITION_PATH + " from " + HUDI + "_trips_snapshot")
      .limit(2)
      .collectAsList()

    // issue deletes
    val deletes = dataGen.generateDeletes(ds)
    println(s"deletes ${deletes}")
    val df = spark.read.json(spark.sparkContext.parallelize(deletes, 2))

    df.write
      .format(HUDI)
      .options(getQuickstartWriteConfigs)
      .option(OPERATION_OPT_KEY, "delete")
      .option(PRECOMBINE_FIELD_OPT_KEY, TIMESTAMP)
      .option(RECORDKEY_FIELD_OPT_KEY, UUID)
      .option(PARTITIONPATH_FIELD_OPT_KEY, PARITION_PATH)
      .option(TABLE_NAME, tableName)
      .mode(Append)
      .save(basePath)

    // fetch should return (total - 2) records
    createSnapshotView(spark, basePath)
    val count = sql("select " + UUID + ", " + PARITION_PATH + " from " + HUDI + "_trips_snapshot").count()
    println(s"after delete count:${count}")
  }

  private def insertOverwriteTable(spark: SparkSession, tableName: String, basePath: String, dataGen: DataGenerator): Unit = {
    println("before overwrite table")
    spark.read.format(HUDI).load(basePath + "/*/*/*/*").select(UUID, PARITION_PATH).show(10, truncate = false)

    val inserts = convertToStringList(dataGen.generateInserts(10))
    val df = spark.read.json(spark.sparkContext.parallelize(inserts, 2))
    df.write.format(HUDI)
      .options(getQuickstartWriteConfigs)
      .option(OPERATION_OPT_KEY, INSERT_OVERWRITE_TABLE_OPERATION_OPT_VAL)
      .option(PRECOMBINE_FIELD_OPT_KEY, TIMESTAMP)
      .option(RECORDKEY_FIELD_OPT_KEY, UUID)
      .option(PARTITIONPATH_FIELD_OPT_KEY, PARITION_PATH)
      .option(TABLE_NAME, tableName)
      .mode(Append)
      .save(basePath)

    // Should have different keys now, from query before.
    println("after overwrite table")
    spark.read.format(HUDI).load(basePath + "/*/*/*/*").select(UUID, PARITION_PATH).show(10, truncate = false)
  }

  private def insertOverwrite(spark: SparkSession, tableName: String, basePath: String, dataGen: DataGenerator): Unit = {
    println("before insertOverwrite")
    showUuidPatitionPath(spark, basePath)

    val inserts = convertToStringList(dataGen.generateInserts(10))
    spark.read
      .json(spark.sparkContext.parallelize(inserts, 2))
      .filter(PARITION_PATH + " = 'americas/united_states/san_francisco'")
      .write.format(HUDI).options(getQuickstartWriteConfigs)
      .option(OPERATION_OPT_KEY, INSERT_OPERATION_OPT_VAL)
      .option(PRECOMBINE_FIELD_OPT_KEY, TIMESTAMP)
      .option(RECORDKEY_FIELD_OPT_KEY, UUID)
      .option(PARTITIONPATH_FIELD_OPT_KEY, PARITION_PATH)
      .option(TABLE_NAME, tableName)
      .mode(Append)
      .save(basePath)

    // Should have different keys now for San Francisco alone, from query before.
    println("after insertOverwrite")
    showUuidPatitionPath(spark, basePath)
  }


  private def showUuidPatitionPath(spark: SparkSession, basePath: String): Unit = {
    spark.read.format(HUDI).load(basePath + "/*/*/*/*").select(UUID, PARITION_PATH).sort(PARITION_PATH, UUID).show(100, false)
  }
}
