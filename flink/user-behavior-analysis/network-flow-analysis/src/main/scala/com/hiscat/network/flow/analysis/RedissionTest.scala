package com.hiscat.network.flow.analysis

import org.redisson.Redisson

object RedissionTest {
  def main(args: Array[String]): Unit = {
    import org.redisson.config.Config
    // 1. Create config object// 1. Create config object

    val config = new Config
    config.useSingleServer()
      .setAddress("redis://hadoop102:6379")

    val client = Redisson.create(config)

    val count = client.getMap[Long, Long]("count")

    val bit = client.getBitSet("bit")
    bit.set(0, true)
    //    client.shutdown()

  }
}
