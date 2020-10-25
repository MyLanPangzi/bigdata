package com.hiscat.flink.user.behavior.analysis

import java.sql.Timestamp

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.math.Ordering

/**
 * 每隔 5 分钟输出最近一小时内点击量最多的前 N 个商品。
 * 将这个需求进行分解我们大概要做这么几件事情：
 * • 抽取出业务时间戳， 告诉 Flink 框架基于业务时间做窗口
 * • 过滤出点击行为数据
 * • 按一小时的窗口大小， 每 5 分钟统计一次， 做滑动窗口聚合（ Sliding Window）
 * • 按每个窗口聚合， 输出每个窗口中点击量前 N 名的商品
 */
object HotItemAnalysis {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(4)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    env.readTextFile("mock-data/UserBehavior.csv")
      .map(e => {
        val split = e.split(",")
        UserBehavior(split(0).toLong, split(1).toLong, split(2).toLong, split(3), split(4).toLong)
      })
      .assignAscendingTimestamps(_.ts * 1000L)
      .filter(e => e.behavior == "pv")
      .keyBy(_.productId)
      .timeWindow(Time.hours(1), Time.minutes(5))
      .aggregate(
        new ItemCountAgg(),
        (_: Long, w: TimeWindow, i: Iterable[ItemViewCount], out: Collector[ItemViewCount]) => {
          i.head.windowEnd = w.getEnd
          out.collect(i.head)
        }
      )
      .keyBy(_.windowEnd)
      .process(new TopN(5))
      .print()

    env.execute("hot item analysis")
  }

  class ItemCountAgg extends AggregateFunction[UserBehavior, ItemViewCount, ItemViewCount] {
    override def createAccumulator(): ItemViewCount = ItemViewCount(0, 0, 0)

    override def add(value: UserBehavior, accumulator: ItemViewCount): ItemViewCount = {
      accumulator.count += 1
      accumulator.itemId = value.productId
      accumulator
    }

    override def getResult(accumulator: ItemViewCount): ItemViewCount = accumulator

    override def merge(a: ItemViewCount, b: ItemViewCount): ItemViewCount = {
      a.count += b.count
      a
    }
  }

  case class TopN(topN: Int) extends KeyedProcessFunction[Long, ItemViewCount, String] {
    lazy val itemsState: MapState[Long, Long] =
      getRuntimeContext.getMapState[Long, Long](
        new MapStateDescriptor[Long, Long]("itemRank", classOf[Long], classOf[Long])
      )

    override def processElement(value: ItemViewCount,
                                ctx: KeyedProcessFunction[Long, ItemViewCount, String]#Context,
                                out: Collector[String]): Unit = {
      itemsState.put(value.itemId, value.count)
      ctx.timerService().registerEventTimeTimer(value.windowEnd + 1)
    }

    override def onTimer(timestamp: Long,
                         ctx: KeyedProcessFunction[Long, ItemViewCount, String]#OnTimerContext,
                         out: Collector[String]): Unit = {

      import scala.collection.JavaConverters._
      val r = itemsState.entries().asScala.toList
        .map(e => (e.getKey, e.getValue))
        .sortBy(_._2)(Ordering.Long.reverse)
        .take(topN)
        .mkString("\n")
      out.collect(
        s"""
           |time:${new Timestamp(timestamp)}
           |${r}
           |
           |""".stripMargin
      )
      itemsState.clear()
    }

  }


}
