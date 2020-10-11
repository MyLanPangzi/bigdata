package com.hiscat.flink.scala.sink

import com.hiscat.flink.scala.WordCount
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink
import org.apache.http.HttpHost
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests

object EsSink {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val httpHosts = new java.util.ArrayList[HttpHost]
    httpHosts.add(new HttpHost("localhost", 9200, "http"))

    val esSinkBuilder = new ElasticsearchSink.Builder[WordCount](
      httpHosts,
      (element: WordCount, _: RuntimeContext, indexer: RequestIndexer) => {
        val json = new java.util.HashMap[String, String]
        json.put("word", element.word)
        json.put("count", element.count.toString)
        json.put("ts", element.ts.toString)

        val rqst: IndexRequest = Requests.indexRequest
          .index("wc")
          .source(json)

        indexer.add(rqst)
      }
    )

    // configuration for the bulk requests; this instructs the sink to emit after every element, otherwise they would be buffered
    esSinkBuilder.setBulkFlushMaxActions(1)
    env.fromElements(
      WordCount("hello", 1, 1),
      WordCount("world", 1, 1),
    )
      .addSink(esSinkBuilder.build())

    env.execute("es sink")
  }

}
