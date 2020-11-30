package com.hiscat.flink.connector.jdbc

case class JdbcConnectionOptions(
                                  url: String,
                                  driver: String,
                                  tableName: String,
                                  enableUpsert: Boolean,
                                  username: Option[String],
                                  password: Option[String]
                                )
