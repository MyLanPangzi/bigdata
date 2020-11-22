package com.hiscat.realtime.analysis.connector.redis

import java.util.concurrent.TimeUnit

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.flink.api.common.serialization.SerializationSchema
import org.apache.flink.configuration.Configuration
import org.apache.flink.metrics.MetricGroup
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.table.data.RowData
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks

case class RedisSinkFunction(redisOptions: RedisOptions,
                             serializer: SerializationSchema[RowData],
                             keys: Iterable[String]
                            ) extends RichSinkFunction[RowData] {
  lazy val jedis = new Jedis(redisOptions.host, redisOptions.port)

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    serializer.open(
      new SerializationSchema.InitializationContext {
        override def getMetricGroup: MetricGroup = getRuntimeContext.getMetricGroup
      }
    )
  }

  override def invoke(value: RowData, context: SinkFunction.Context[_]): Unit = {
    val json = JSON.parseObject(new String(serializer.serialize(value)))
    val mapKey = getMapKey(json)
    if (jedis.exists(mapKey)) {
      jedis.hset(mapKey, getDataKey(json), json.toString())
    } else {
      jedis.hset(mapKey, getDataKey(json), json.toString())
      jedis.expire(mapKey, TimeUnit.DAYS.toSeconds(1).intValue())
    }
  }

  private def getMapKey(json: JSONObject): String = {
    val vars = extractVars(redisOptions.mapKey)
    var mapKey = redisOptions.mapKey
    vars.foreach(i => {
      val key = i.replace("{", "").replace("}", "")
      val v = json.getString(key)
      mapKey = mapKey.replace(i, if (v == null) "" else v)
    })
    mapKey
  }

  private def extractVars(format: String): ListBuffer[String] = {
    var r: ListBuffer[String] = ListBuffer()
    var start = 0;
    Breaks.breakable {
      while (start < format.length) {
        start = format.indexOf("{", start)
        val end = format.indexOf("}", start)
        if (start <= 0) {
          Breaks.break
        }
        if (end <= 0) {
          throw new IllegalArgumentException("no match }")
        }
        r += format.substring(start, end + 1)
        start = end + 1
      }
    }
    r
  }

  private def getDataKey(json: JSONObject) = {
    import scala.collection.JavaConverters._;
    val list = keys.toList
    json
      .asScala
      .filter {
        case (k, _) =>
          list.contains(k)
      }
      .values
      .mkString(redisOptions.keyDelimiter)
  }

  override def close(): Unit = {
    jedis.close()
  }
}
