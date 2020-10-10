package com.hiscat.flink.scala

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._

object SocketStreamWordCount {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
//    env.disableOperatorChaining()


    val arg = ParameterTool.fromArgs(args)

    env.socketTextStream(arg.get("host"), arg.getInt("port"))
      .filter(_.nonEmpty).disableChaining()
      .flatMap(_.split(" ")).startNewChain()
      .map((_, 1)).slotSharingGroup("test")
      .keyBy(_._1)
      .sum(1)
      .print().setParallelism(1)

    env.execute("socket stream word count")
  }
}
