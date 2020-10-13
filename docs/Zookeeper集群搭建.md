[Parent](../README.md)
<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->

- [Zookeeper集群搭建](#zookeeper%E9%9B%86%E7%BE%A4%E6%90%AD%E5%BB%BA)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->


# Zookeeper集群搭建

## JDK

## ZK

```shell script
curl https://mirror.bit.edu.cn/apache/zookeeper/zookeeper-3.6.2/apache-zookeeper-3.6.2-bin.tar.gz \
-o apache-zookeeper-3.6.2-bin.tar.gz
tar zxf apache-zookeeper-3.6.2-bin.tar.gz -C /opt/module
cd /opt/module
mv apache-zookeeper-3.6.2-bin zookeeper
cd /opt/module/zookeeper/conf
mv zoo_sample.cfg zoo.cfg
vi zoo.cfg
# dataDir=/opt/module/zookeeper/data
#server.1=yh001:2888:3888
# server.2=yh002:2888:3888
# server.3=yh003:2888:3888

mkdir /opt/module/zookeeper/data
touch /opt/module/zookeeper/data/myid
echo 1 > /opt/module/zookeeper/data/myid
xsync.sh /opt/module/zookeeper
# 修改其余节点myid文件
# 编写集群启动脚本
```
