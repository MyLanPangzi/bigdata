package com.hiscat.flink.user.behavior.analysis

  case class UserBehavior(userId: Long, productId: Long, categoryId: Long, behavior: String, ts: Long)
