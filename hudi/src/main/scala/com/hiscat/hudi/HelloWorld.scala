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
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("test")
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()
    import spark.implicits._

    val tableName = "hudi_trips_cow"
    val basePath = "file:///E:/github/bigdata/tmp/"
    val dataGen = new DataGenerator()
    spark
      .read
      .format("hudi")
      .load(basePath + "/*/*/*/*")
      .createOrReplaceTempView("hudi_trips_snapshot")

    //    insert(spark, tableName, basePath, dataGen)
    //    snapshotQuery(spark, basePath)
    //    update(spark, tableName, basePath, dataGen)
    //    incrementalQuery(spark, basePath)
    //    pointTimeRead(spark, basePath)
    //    delete(spark, tableName, basePath, dataGen)
    //    insertOverwriteTable(spark, tableName, basePath, dataGen)
    //    insertOverwrite(spark, tableName, basePath, dataGen)
  }

  private def insertOverwrite(spark: SparkSession, tableName: String, basePath: String, dataGen: DataGenerator): Unit = {
    spark.
      read.format("hudi").
      load(basePath + "/*/*/*/*").
      select("uuid", "partitionpath").
      sort("partitionpath", "uuid").
      show(100, false)

    val inserts = convertToStringList(dataGen.generateInserts(10))
    val df = spark.
      read.json(spark.sparkContext.parallelize(inserts, 2)).
      filter("partitionpath = 'americas/united_states/san_francisco'")
    df.write.format("hudi").
      options(getQuickstartWriteConfigs).
      option(OPERATION_OPT_KEY, "insert_overwrite").
      option(PRECOMBINE_FIELD_OPT_KEY, "ts").
      option(RECORDKEY_FIELD_OPT_KEY, "uuid").
      option(PARTITIONPATH_FIELD_OPT_KEY, "partitionpath").
      option(TABLE_NAME, tableName).
      mode(Append).
      save(basePath)

    // Should have different keys now for San Francisco alone, from query before.
    spark.
      read.format("hudi").
      load(basePath + "/*/*/*/*").
      select("uuid", "partitionpath").
      sort("partitionpath", "uuid").
      show(100, false)
  }

  private def insertOverwriteTable(spark: SparkSession, tableName: String, basePath: String, dataGen: DataGenerator): Unit = {
    spark.
      read.format("hudi").
      load(basePath + "/*/*/*/*").
      select("uuid", "partitionpath").
      show(10, false)

    val inserts = convertToStringList(dataGen.generateInserts(10))
    val df = spark.read.json(spark.sparkContext.parallelize(inserts, 2))
    df.write.format("hudi").
      options(getQuickstartWriteConfigs).
      option(OPERATION_OPT_KEY, "insert_overwrite_table").
      option(PRECOMBINE_FIELD_OPT_KEY, "ts").
      option(RECORDKEY_FIELD_OPT_KEY, "uuid").
      option(PARTITIONPATH_FIELD_OPT_KEY, "partitionpath").
      option(TABLE_NAME, tableName).
      mode(Append).
      save(basePath)

    // Should have different keys now, from query before.
    spark.
      read.format("hudi").
      load(basePath + "/*/*/*/*").
      select("uuid", "partitionpath").
      show(10, false)
  }

  private def delete(spark: SparkSession, tableName: String, basePath: String, dataGen: DataGenerator) = {
    spark.sql("select uuid, partitionpath from hudi_trips_snapshot").count()
    // fetch two records to be deleted
    val ds = spark.sql("select uuid, partitionpath from hudi_trips_snapshot").limit(2)

    // issue deletes
    val deletes = dataGen.generateDeletes(ds.collectAsList())
    val df = spark.read.json(spark.sparkContext.parallelize(deletes, 2))

    df.write.format("hudi").
      options(getQuickstartWriteConfigs).
      option(OPERATION_OPT_KEY, "delete").
      option(PRECOMBINE_FIELD_OPT_KEY, "ts").
      option(RECORDKEY_FIELD_OPT_KEY, "uuid").
      option(PARTITIONPATH_FIELD_OPT_KEY, "partitionpath").
      option(TABLE_NAME, tableName).
      mode(Append).
      save(basePath)

    // run the same read query as above.
    val roAfterDeleteViewDF = spark.read.format("hudi").
      load(basePath + "/*/*/*/*")

    roAfterDeleteViewDF.registerTempTable("hudi_trips_snapshot")
    // fetch should return (total - 2) records
    spark.sql("select uuid, partitionpath from hudi_trips_snapshot").count()
  }

  private def pointTimeRead(spark: SparkSession, basePath: String): Unit = {

    val commits = spark
      .sql("select distinct(_hoodie_commit_time) as commitTime from  hudi_trips_snapshot order by commitTime")
      .map(k => k.getString(0))(ExpressionEncoder()).take(50)
    val beginTime = "000" // Represents all commits > this time.
    val endTime = commits(commits.length - 2) // commit time we are interested in

    //incrementally query data
    val tripsPointInTimeDF = spark.read.format("hudi").
      option(QUERY_TYPE_OPT_KEY, QUERY_TYPE_INCREMENTAL_OPT_VAL).
      option(BEGIN_INSTANTTIME_OPT_KEY, beginTime).
      option(END_INSTANTTIME_OPT_KEY, endTime).
      load(basePath)
    tripsPointInTimeDF.createOrReplaceTempView("hudi_trips_point_in_time")
    spark.sql("select `_hoodie_commit_time`, fare, begin_lon, begin_lat, ts from hudi_trips_point_in_time where fare > 20.0").show()
  }

  private def update(spark: SparkSession, tableName: String, basePath: String, dataGen: DataGenerator): Unit = {
    val updates = convertToStringList(dataGen.generateInserts(10))
    val df = spark.read.json(spark.sparkContext.parallelize(updates, 2))
    df.write.format("hudi").
      options(getQuickstartWriteConfigs).
      option(PRECOMBINE_FIELD_OPT_KEY, "ts").
      option(RECORDKEY_FIELD_OPT_KEY, "uuid").
      option(PARTITIONPATH_FIELD_OPT_KEY, "partitionpath").
      option(TABLE_NAME, tableName).
      mode(Append).
      save(basePath)
  }

  private def snapshotQuery(spark: SparkSession, basePath: String): Unit = {
    val tripsSnapshotDF = spark.
      read.
      format("hudi").
      load(basePath + "/*/*/*/*")
    //load(basePath) use "/partitionKey=partitionValue" folder structure for Spark auto partition discovery
    tripsSnapshotDF.createOrReplaceTempView("hudi_trips_snapshot")

    spark.sql("select fare, begin_lon, begin_lat, ts from  hudi_trips_snapshot where fare > 20.0").show()
    spark.sql("select _hoodie_commit_time, _hoodie_record_key, _hoodie_partition_path, rider, driver, fare from  hudi_trips_snapshot").show()
  }

  private def insert(spark: SparkSession, tableName: String, basePath: String, dataGen: DataGenerator): Unit = {
    val list = dataGen.generateInserts(10)
    val inserts = convertToStringList(list)
    val df = spark.read.json(spark.sparkContext.parallelize(inserts, 2))
    df.write.format("hudi").
      options(getQuickstartWriteConfigs).
      option(PRECOMBINE_FIELD_OPT_KEY, "ts").
      option(RECORDKEY_FIELD_OPT_KEY, "uuid").
      option(PARTITIONPATH_FIELD_OPT_KEY, "partitionpath").
      option(TABLE_NAME, tableName).
      mode(Overwrite).
      save(basePath)
  }

  private def incrementalQuery(spark: SparkSession, basePath: String): Unit = {


    val commits = spark
      .sql("select distinct(_hoodie_commit_time) as commitTime from  hudi_trips_snapshot order by commitTime")
      .map(k => k.getString(0))(ExpressionEncoder()).take(50)
    val beginTime = commits(commits.length - 2) // commit time we are interested in

    // incrementally query data
    val tripsIncrementalDF = spark.read.format("hudi").
      option(QUERY_TYPE_OPT_KEY, QUERY_TYPE_INCREMENTAL_OPT_VAL).
      option(BEGIN_INSTANTTIME_OPT_KEY, beginTime).
      load(basePath)
    tripsIncrementalDF.createOrReplaceTempView("hudi_trips_incremental")

    spark.sql("select `_hoodie_commit_time`, fare, begin_lon, begin_lat, ts from  hudi_trips_incremental where fare > 20.0").show()
  }
}
