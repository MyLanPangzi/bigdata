package com.hiscat.realtime.analysis.connector.redis

case class RedisOptions(
                         host: String,
                         port: Int,
                         mapKey: String,
                         keyDelimiter: String
                       )
