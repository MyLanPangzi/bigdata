[Parent](../README.md)

# Flink

* 流处理框架
* 流处理引擎
* 状态计算
* 无边界数据流
* 有边界数据流

## 特点

* 低延迟
* 高吞吐
* 容错
* 准确处理
* 事件驱动
* 流的世界观
* 分层API
* 丰富的时间语义
* exactly once
* high reliable
* connectors

## 比较Spark Streaming

### 架构

spark:
* 批处理
* 划分Stage

Flink:
* 流处理
* 事件驱动


### 数据模型

spark：
* RDD
* DataSet/DataFrame

Flink：
* SQL
* Table API
* DataStream/DataSet
* Stateful Process Function