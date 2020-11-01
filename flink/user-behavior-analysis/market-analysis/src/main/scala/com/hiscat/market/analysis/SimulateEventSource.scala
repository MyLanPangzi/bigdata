package com.hiscat.market.analysis

import java.util.UUID

import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}

import scala.util.Random

class SimulateEventSource extends RichParallelSourceFunction[MarketingUserBehavior] {
  var running = true

  val channelSet: Seq[String] = Seq("AppStore", "XiaomiStore", "HuaweiStore", "weibo",
    "wechat", "tieba")
  val behaviorTypes: Seq[String] = Seq("BROWSE", "CLICK", "PURCHASE", "UNINSTALL")
  val rand: Random = Random

  override def run(ctx: SourceFunction.SourceContext[MarketingUserBehavior]): Unit = {
    while (running) {
      ctx.collect(
        MarketingUserBehavior(UUID.randomUUID().toString,
          behaviorTypes(rand.nextInt(behaviorTypes.size)),
          channelSet(rand.nextInt(channelSet.size)),
          System.currentTimeMillis()
        )
      )
      Thread.sleep(100)
    }
  }

  override def cancel(): Unit = running = false
}
