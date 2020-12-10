package com.hiscat.flink.connector.redis

import org.apache.flink.api.common.serialization.SerializationSchema
import org.apache.flink.configuration.Configuration
import org.apache.flink.metrics.MetricGroup
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.table.data.{RowData, StringData}
import org.apache.flink.table.types.DataType
import org.apache.flink.util.UserCodeClassLoader
import redis.clients.jedis.Jedis

import scala.collection.mutable

case class RedisSinkFunction(
                              options: RedisOptions,
                              serializer: SerializationSchema[RowData],
                              pks: mutable.Seq[Int],
                              pkTypes: mutable.Seq[DataType]
                            ) extends RichSinkFunction[RowData] {
  //  lazy val config = new Config
  //  lazy val redisson: RedissonClient = Redisson.create(config)
  lazy private val jedis = new Jedis(options.hostname, options.port)

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    //    config.useSingleServer().setAddress(s"redis://${options.hostname}:${options.port}")
    serializer.open(
      new SerializationSchema.InitializationContext {
        override def getMetricGroup: MetricGroup = getRuntimeContext.getMetricGroup

        override def getUserCodeClassLoader: UserCodeClassLoader =
          getRuntimeContext.getUserCodeClassLoader.asInstanceOf[UserCodeClassLoader]
      }
    )
  }

  override def invoke(value: RowData, context: SinkFunction.Context): Unit = {
    val key = pks.zipWithIndex
      .map {
        case (e, i) =>
          RowData.createFieldGetter(pkTypes(i).getLogicalType, e).getFieldOrNull(value)
      }
      .map {
        case e: StringData => new String(e.toBytes)
        case e => e
      }
      .mkString(options.keyDelimiter)
    jedis.hset(options.hashKey, key, new String(serializer.serialize(value)))
    //    redisson.getMap(options.hashKey).put(key, new String(serializer.serialize(value)))
  }

  override def close(): Unit = {
    //    redisson.shutdown()
    jedis.close()
  }
}
