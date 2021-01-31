package com.hiscat.realtime.analysis.connector

case class HudiTableOptions(
                             basePath: String,
                             tableName: String,
                             preCombineKey: String,
                             schema: String
                           )
