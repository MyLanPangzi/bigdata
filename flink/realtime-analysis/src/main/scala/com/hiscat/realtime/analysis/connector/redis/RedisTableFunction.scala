package com.hiscat.realtime.analysis.connector.redis

import org.apache.flink.api.common.serialization.DeserializationSchema
import org.apache.flink.metrics.MetricGroup
import org.apache.flink.table.data.{GenericRowData, RowData}
import org.apache.flink.table.functions.{FunctionContext, TableFunction}
import redis.clients.jedis.Jedis

case class RedisTableFunction(redisOptions: RedisOptions,
                              deserializer: DeserializationSchema[RowData],
                              keys: Iterable[String]) extends TableFunction[RowData] {
  lazy val jedis = new Jedis(redisOptions.host, redisOptions.port)

  def eval(keys: AnyRef*): Unit = {
    val rowData = GenericRowData.of(keys)
//    RowData.createFieldGetter(,0)
  }

  override def open(context: FunctionContext): Unit = {

  }

  override def close(): Unit = {
    jedis.close()
  }
}
