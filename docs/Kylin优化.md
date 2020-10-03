[Parent](../README.md)

# Kylin优化

## 维度设计优化

### 必需维度

维度：A，B，C，不包含这三个维度的Cuboid都不要


### 层级维度

维度：A-B-C，不会单独出现B，C，BC，AC的Cuboid

* A
* A B
* A B C

### 联合维度

维度：[AB]，不会单独出现A，或者B的Cuboid

### 衍生列

不会构建衍生列的cuboid，查询时会加载维表到内存中进行映射，会增加查询时间，减少构建时间，减少磁盘占用。

## 构建优化

### 构建步骤

1. 打平表 
1. 再分发，
    * 根据高基数列分区distribute by
    * 减少mapper输入行数，提高并行度，默认100万行
1. 统计维度列 
1. 构建维度字典 
1. 保存统计 
1. 创建HTable 
1. 构建Cube/Cuboid，根据情况二选一
    1. by-layer 层级构建，首先构建Base Cuboid，逐层构建N-Cuboid，
        * 减少reducer输入大小，提高并行度，默认500M
    1. by-split 快速构建，基于内存构建，mapper端聚合，非combiner，优化内存配置，默认mapper内存3个G
1. 转换Cuboid到HFile
    * 调整region，split大小，默认5G
    * 调整HFile大小，默认2G
1. 写入HBase Region
1. 更新Cube信息
1. 清理Hive中间表
1. 垃圾回收

### Rowkey

这里调的就是字典序

* 过滤条件放在前面
* 基数大的列放在前面