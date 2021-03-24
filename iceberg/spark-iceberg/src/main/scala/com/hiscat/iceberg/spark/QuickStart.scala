package com.hiscat.iceberg.spark

import org.apache.spark.sql.SparkSession

object QuickStart {

  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdp")
    val spark = SparkSession.builder()
      .appName("iceberg quick start")
      .master("local[1]")
      .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
      .config("spark.sql.catalog.spark_catalog.type", "hive")
      .config("spark.sql.catalog.hadoop", "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.hadoop.type", "hadoop")
      //      .config("spark.sql.catalog.hadoop.warehouse", "hdfs://yh001:9820/warehouse")
      .config("spark.sql.catalog.hadoop.warehouse", "/warehouse")
      //      .enableHiveSupport()
      .getOrCreate
    testDDL(spark)
    testWrite(spark)

    spark.stop()
  }

  private def testDDL(spark: SparkSession) = {
    import spark._

    sql("USE hadoop.default")
    sql("DROP TABLE IF EXISTS test")
    sql("DROP TABLE IF EXISTS t2")
    sql("CREATE TABLE IF NOT EXISTS test(id BIGINT NOT NULL, name STRING, height INT, weight FLOAT, ts TIMESTAMP) USING iceberg PARTITIONED BY(truncate(10, id))")
    sql("CREATE TABLE IF NOT EXISTS test(id BIGINT, name STRING, ts TIMESTAMP) USING iceberg PARTITIONED BY(truncate(10, name))")
    sql("CREATE TABLE IF NOT EXISTS test(id BIGINT, name STRING, ts TIMESTAMP) USING iceberg PARTITIONED BY(bucket(10, name))")
    sql("CREATE TABLE IF NOT EXISTS test(id BIGINT, name STRING, ts TIMESTAMP) USING iceberg PARTITIONED BY(years(ts))")
    sql("CREATE TABLE IF NOT EXISTS test(id BIGINT, name STRING, ts TIMESTAMP) USING iceberg PARTITIONED BY(months(ts))")
    sql("CREATE TABLE IF NOT EXISTS test(id BIGINT, name STRING, ts TIMESTAMP) USING iceberg PARTITIONED BY(days(ts))")

    sql("CREATE TABLE IF NOT EXISTS t2 USING iceberg AS SELECT * FROM test")
    sql("CREATE OR REPLACE TABLE t2 USING iceberg AS SELECT * FROM test")
    sql("REPLACE TABLE t2 USING iceberg AS SELECT * FROM test")

    //    sql("ALTER TABLE t2 RENAME TO t3") //Cannot rename Hadoop tables
    sql("ALTER TABLE t2 SET TBLPROPERTIES('test'='test')")
    sql("ALTER TABLE t2 UNSET TBLPROPERTIES('test')")
    sql("ALTER TABLE t2 ADD COLUMNS (gender STRING)")
    sql("ALTER TABLE t2 ADD COLUMN address STRUCT<lon: double, lat: double> ")
    sql("ALTER TABLE t2 ADD COLUMN address.remark STRING ")
    sql("ALTER TABLE t2 ADD COLUMN birthday DATE AFTER gender ")
    sql("ALTER TABLE t2 ADD COLUMN key BIGINT FIRST ")

    sql("ALTER TABLE t2 RENAME COLUMN key TO key_deprecated ")
    sql("ALTER TABLE t2 RENAME COLUMN address.remark TO comment ")
    sql("ALTER TABLE t2 RENAME COLUMN address TO addr ")

    sql("ALTER TABLE t2 ALTER COLUMN height TYPE BIGINT ")
    sql("ALTER TABLE t2 ALTER COLUMN weight TYPE DOUBLE ")
    sql("ALTER TABLE t2 ALTER COLUMN weight COMMENT 'weight' ")
    sql("ALTER TABLE t2 ALTER COLUMN weight FIRST ")
    sql("ALTER TABLE t2 ALTER COLUMN height AFTER weight ")
    sql("ALTER TABLE t2 ALTER COLUMN id DROP NOT NULL ")

    sql("ALTER TABLE t2 DROP COLUMN addr.lat ")
    sql("ALTER TABLE t2 DROP COLUMN addr ")

    //    org.apache.spark.sql.catalyst.parser.UpperCaseCharStream.<init>(Lorg/apache/iceberg/shaded/org/antlr/v4/runtime/CodePointCharStream;)V
    sql("ALTER TABLE t2 ADD PARTITION FIELD gender ")
    sql("ALTER TABLE t2 ADD PARTITION FIELD bucket(16, name) ")
    sql("ALTER TABLE t2 ADD PARTITION FIELD years(birthday) ")

    sql("ALTER TABLE t2 DROP PARTITION FIELD gender ")
    sql("ALTER TABLE t2 DROP PARTITION FIELD bucket(16, name) ")
    sql("ALTER TABLE t2 DROP PARTITION FIELD years(birthday) ")

    sql("ALTER TABLE t2 WRITE ORDERED BY birthday ASC, height DESC NULL FIRST, weight ASC NULL LAST")
  }

  def testWrite(spark: SparkSession): Unit = {
    import spark._
    sql("USE hadoop.default")
    sql("CREATE TABLE IF NOT EXISTS t3()")
  }
}
