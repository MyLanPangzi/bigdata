package com.hiscat.flink.connector.phoenix

import org.apache.flink.table.connector.source.{DynamicTableSource, LookupTableSource, TableFunctionProvider}

case class PhoenixDynamicLookupTableSource(
                                            phoenixOptions: PhoenixOptions,
                                            pks: Array[String]
                                          ) extends LookupTableSource {
  override def getLookupRuntimeProvider(context: LookupTableSource.LookupContext): LookupTableSource.LookupRuntimeProvider = {
    println("look up")
    TableFunctionProvider.of(PhoenixLookupTableFunction(phoenixOptions, pks))
  }

  override def copy(): DynamicTableSource = PhoenixDynamicLookupTableSource(phoenixOptions, pks)

  override def asSummaryString(): String = "phoenix look up table source"
}
