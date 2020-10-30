package com.hiscat.flink.user.behavior

package object analysis {

  case class ItemViewCount(var windowEnd: Long,var itemId: Long,var count: Long)

}
