package com.hiscat.realtime.analysis.connector

import org.apache.flink.api.common.serialization.SerializationSchema
import org.apache.flink.metrics.MetricGroup
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.table.data.RowData.FieldGetter
import org.apache.flink.table.data.{RowData, TimestampData}
import org.apache.flink.util.UserCodeClassLoader
import org.apache.hadoop.conf.Configuration
import org.apache.hudi.client.HoodieJavaWriteClient
import org.apache.hudi.client.common.HoodieJavaEngineContext
import org.apache.hudi.common.HoodieJsonPayload
import org.apache.hudi.common.model.{HoodieKey, HoodieRecord}
import org.apache.hudi.config.{HoodieCompactionConfig, HoodieIndexConfig, HoodieWriteConfig}
import org.apache.hudi.index.HoodieIndex

import java.util.Collections
import scala.collection.mutable

case class HudiDynamicTableSinkFunction(
                                         hudiTableOptions: HudiTableOptions,
                                         serializer: SerializationSchema[RowData],
                                         pkGetters: mutable.Seq[FieldGetter],
                                         partitionGetters: mutable.Seq[FieldGetter],
                                         preCombineKeyGetter: FieldGetter
                                       ) extends RichSinkFunction[RowData] {

  lazy private val conf = new Configuration
  lazy private val cfg = HoodieWriteConfig.newBuilder
    .withPath(hudiTableOptions.basePath)
    .forTable(hudiTableOptions.tableName)
    .withSchema(hudiTableOptions.schema)
    .withDeleteParallelism(2)
    .withParallelism(2, 2)
    .withIndexConfig(HoodieIndexConfig.newBuilder.withIndexType(HoodieIndex.IndexType.INMEMORY).build)
    .withCompactionConfig(HoodieCompactionConfig.newBuilder.archiveCommitsWith(20, 30).build)
    .build
  lazy private val client: HoodieJavaWriteClient[HoodieJsonPayload] = new HoodieJavaWriteClient[HoodieJsonPayload](
    new HoodieJavaEngineContext(conf), cfg
  )

  override def open(parameters: org.apache.flink.configuration.Configuration): Unit = {
    serializer.open(
      new SerializationSchema.InitializationContext {
        override def getMetricGroup: MetricGroup = getRuntimeContext.getMetricGroup

        override def getUserCodeClassLoader: UserCodeClassLoader =
          getRuntimeContext.getUserCodeClassLoader.asInstanceOf[UserCodeClassLoader]
      }
    )
  }

  override def invoke(value: RowData, context: SinkFunction.Context): Unit = {
    val json = new String(serializer.serialize(value))
    val key = pkGetters.map(e => e.getFieldOrNull(value)).mkString("_")
    val partition = partitionGetters.map(e => e.getFieldOrNull(value)).mkString("/")
    val data = preCombineKeyGetter.getFieldOrNull(value)
    val preCombineKey = data match {
      case t: TimestampData => t.getMillisecond
      case l: java.lang.Long => l
      case _ => throw new RuntimeException("unsupported pre combine key type")
    }
    val instantTime = client.startCommit()
    //    client.upsert()
    println(s"hudi invoke ${key} ${partition} ${preCombineKey} ${json}")
    val statuses = client.upsert(
      Collections.singletonList(
        new HoodieRecord(
          new HoodieKey(key, partition),
          new HoodieJsonPayload(json)
        )
      ),
      instantTime
    )
    client.commit(instantTime, statuses)

  }

  override def close(): Unit = {
    client.close()
  }
}
