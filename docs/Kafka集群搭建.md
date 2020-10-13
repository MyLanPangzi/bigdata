[Parent](../README.md)

# Kafka

```shell script
curl https://mirrors.tuna.tsinghua.edu.cn/apache/kafka/2.6.0/kafka_2.13-2.6.0.tgz \
-o kafka_2.13-2.6.0.tgz 
tar zxf kafka_2.13-2.6.0.tgz -C ../module
cd ../module
mv kafka_2.13-2.6.0 kafka
cd kafka
vi config/server.properties
# 21 line 修改每个节点的ID，从0开始，依次递增
#编写集群启动脚本
```
