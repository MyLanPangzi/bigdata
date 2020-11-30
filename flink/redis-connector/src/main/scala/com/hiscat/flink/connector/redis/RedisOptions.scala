package com.hiscat.flink.connector.redis


case class RedisOptions(
                         hostname: String,
                         port: Int,
                         hashKey: String,
                         keyDelimiter: String,
                         ttl: Long
                       )
