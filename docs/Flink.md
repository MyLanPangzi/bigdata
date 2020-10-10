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

## 部署模式

差异：
* 集群生命周期，资源隔离保障
* 应用代码（main方法）是在客户端还是集群执行

### 部署最佳实践

提供依赖：
* lib目录提供不可作为插件使用的依赖，注意jar冲突
* plugins目录提供运行时加载的插件，避免类冲突

### 会话模式

优点：
* 每个job无需启动完整的集群组件，资源共享
* job启动时间短，适合短查询应用，注重用户体验

缺点：
* jobmanager的宕机会影响整个集群
* 会引起资源竞争
* job的失败会导致dfs大量并发的访问，从而导致服务不可用
* jobmanager的压力会增大
* 代码执行在客户端，客户端需要消耗大量资源以及带宽

### Per-Job模式

优点：
* job之间资源不共享，更好的资源隔离保障，每个job启动一个集群组件
* 加速job的运行
* 减轻了jobmanager的压力
* 大多数生产环境的首选模式

缺点：
* 代码执行在客户端，客户端需要消耗大量资源以及带宽
* 需要等待资源分配，启动时间长，适合长时间运行的jo，不关心启动时间的job

### 应用模式

优点：
* 为每个应用创建一个会话集群，但是代码执行在Jobmanager上，提供了更好的资源隔离，节省了客户端的资源
* Job会以提交顺序执行，除非调用executeAsync()方法。

缺点：
* 编码时需要额外注意路径问题，必须要Jobmanager能够访问到
* 多执行应用，不支持HA

## 架构

![Flink Architecture](https://ci.apache.org/projects/flink/flink-docs-release-1.11/fig/processes.svg)

### JobManager

至少有一个JobManager，可以HA，一个leader，其余standby。

职责：
* 任务调度
* 响应任务的完成或失败
* 协调checkpoint
* 协调失败恢复


#### ResourceManager

职责：
* 负责分配以及供应资源，管理任务槽（集群调度单元），不同环境有不同实现
* 只分配可用槽，不启动槽

#### Dispatcher

职责：
* 提供REST API提交job，并启动JobMaster
* 运行WebUI

#### JobMaster

* 管理单个JobGraph的执行，每个Job都有自己的JobMaster

### TaskManagers

至少一个TaskManager

职责：
* 执行dataflow的task
* 缓冲并交换数据数据流
* 管理节点上的slot，分配节点上的Managed Memory
* 负责与JobManager通信，报告任务的状态

### 任务槽

* 每个TaskManager就是一个JVM进程，并管理节点上的托管内存，对内存进行隔离，这些隔离后的内存就是任务槽  
* 每个槽对应一个线程，描述了TaskManager的固定资源子集
* 同JVM的任务能共享TCP连接，心跳，数据结构，数据，从而减少任务开销，提供吞吐
* 同Job下的substasks能共享槽，能充分利用资源
* 任务最大的并行度取决于集群的槽数，无需计算每个任务的并行度

dataflow逻辑图

![](https://ci.apache.org/projects/flink/flink-docs-release-1.11/fig/tasks_chains.svg)

槽未共享视图：

![](https://ci.apache.org/projects/flink/flink-docs-release-1.11/fig/tasks_slots.svg)

槽共享后视图：

![](https://ci.apache.org/projects/flink/flink-docs-release-1.11/fig/slot_sharing.svg)
