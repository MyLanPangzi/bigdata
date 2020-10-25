package com.hiscat.flink.user.behavior

package object analysis {

  case class UserBehavior(userId: Long, productId: Long, categoryId: Long, behavior: String, ts: Long)

  case class ItemViewCount(var windowEnd: Long,var itemId: Long,var count: Long)

}
