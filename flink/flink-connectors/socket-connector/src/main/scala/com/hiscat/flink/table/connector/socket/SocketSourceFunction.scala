package com.hiscat.flink.table.connector.socket

import java.io.ByteArrayOutputStream
import java.net.{InetSocketAddress, Socket}

import org.apache.flink.api.common.serialization.DeserializationSchema
import org.apache.flink.api.common.serialization.SerializationSchema.InitializationContext
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.ResultTypeQueryable
import org.apache.flink.configuration.Configuration
import org.apache.flink.metrics.MetricGroup
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.table.data.RowData

case class SocketSourceFunction(hostname: String,
                                port: Integer,
                                byteDelimiter: Byte,
                                deserializer: DeserializationSchema[RowData])
  extends RichSourceFunction[RowData] with ResultTypeQueryable[RowData] {

  var isRunning = true
  var socket: Socket = _

  override def run(ctx: SourceFunction.SourceContext[RowData]): Unit = {
    while (isRunning) {
      socket = new Socket
      socket.connect(
        new InetSocketAddress(hostname, port), 0
      )
      val stream = socket.getInputStream
      val buffer = new ByteArrayOutputStream()
      var b = stream.read()
      while (b >= 0) {
        if (b != byteDelimiter) {
          buffer.write(b)
        } else {
          ctx.collect(deserializer.deserialize(buffer.toByteArray))
          buffer.reset();
        }
        Thread.sleep(1000)
        b = stream.read()
      }
    }
  }

  override def cancel(): Unit = {
    isRunning = false

  }

  override def getProducedType: TypeInformation[RowData] = deserializer.getProducedType

  override def open(parameters: Configuration): Unit = {
    deserializer.open(
      new DeserializationSchema.InitializationContext {
        override def getMetricGroup: MetricGroup = getRuntimeContext.getMetricGroup
      }
    )
  }
}

