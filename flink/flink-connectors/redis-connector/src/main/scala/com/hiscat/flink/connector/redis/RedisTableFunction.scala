package com.hiscat.flink.connector.redis

import org.apache.flink.api.common.serialization.DeserializationSchema
import org.apache.flink.metrics.MetricGroup
import org.apache.flink.table.data.{GenericRowData, RowData}
import org.apache.flink.table.functions.{FunctionContext, TableFunction}
import org.apache.flink.table.types.DataType
import org.apache.flink.util.UserCodeClassLoader
import redis.clients.jedis.Jedis

import scala.annotation.varargs
import scala.collection.mutable

case class RedisTableFunction(
                               options: RedisOptions,
                               deserializer: DeserializationSchema[RowData]
                             ) extends TableFunction[RowData] {
  lazy val jedis = new Jedis(options.hostname, options.port)

  override def open(context: FunctionContext): Unit = {
    deserializer.open(
      new DeserializationSchema.InitializationContext {
        override def getMetricGroup: MetricGroup = context.getMetricGroup

        override def getUserCodeClassLoader: UserCodeClassLoader = null
      }
    )
  }

  @varargs
  def eval(keys: AnyRef*): Unit = {
    val key = keys.mkString(options.keyDelimiter)
    val v = jedis.hget(options.hashKey, key)
    collect(deserializer.deserialize(v.getBytes))
  }

  override def close(): Unit = {
    jedis.close()
  }
}
