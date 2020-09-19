# Hadoop集群搭建

## 步骤描述

* 配置前提条件
* 安装jdk，分发
* 安装hadoop，分发
* 初始化集群
* 编写集群启动脚本，测试
* 读写性能测试

## 前置条件

* 准备三台服务器centos 7
* 三台服务器准备一个账号hadoop，具有root权限，并配置无需密码sudo
    * hadoop102
    * hadoop103
    * hadoop104
* hadoop102配置账号hadoop ssh 到三台服务器
* 确保三台服务器通信正常，防火墙关闭
* rsync工具安装好，curl工具安装好
* 三台服务器建立目录 /opt/software /opt/module确保目录权限都是账户hadoop的

### 安装rsync工具

```shell script
sudo yum install -y rsync.x86_64
```

### 关闭防火墙

```shell script
sudo systemctl disable --now  firewalld.service
```

### Hosts文件配置

```shell script
vi /etc/hosts
127.0.0.1   localhost localhost.localdomain localhost4 localhost4.localdomain4
::1         localhost localhost.localdomain localhost6 localhost6.localdomain6
192.168.242.102 hadoop102
192.168.242.103 hadoop103
192.168.242.104 hadoop104
```

### 编写分发脚本xsync.sh

```shell script
tee ~/bin/xsync.sh
#复制下面的脚本内容，粘贴，然后ctr + d
chmod +x ~/bin/xsync.sh
```

```shell script
#!/bin/bash
#1. 判断参数个数
if [ $# -lt 1 ]
then
  echo Not Enough Arguement!
  exit;
fi
#2. 遍历集群所有机器
for host in hadoop102 hadoop103 hadoop104
do
  echo ====================  $host  ====================
  #3. 遍历所有目录，挨个发送
  for file in $@
  do
    #4 判断文件是否存在
    if [ -e $file ]
    then
      #5. 获取父目录
      pdir=$(cd -P $(dirname $file); pwd)
      #6. 获取当前文件的名称
      fname=$(basename $file)
      ssh $host "mkdir -p $pdir"
      rsync -av $pdir/$fname $host:$pdir
    else
      echo $file does not exists!
    fi
  done
done
```

### 编写集群命令调用脚本

```shell script
#!/usr/bin/env bash

for i in hadoop102 hadoop103 hadoop104 ; do
    ssh -t $i "$@"
done
```

## 安装JDK

```shell script
#https://repo.huaweicloud.com/java/jdk
cd /opt/software
curl https://repo.huaweicloud.com/java/jdk/8u151-b12/jdk-8u151-linux-x64.tar.gz \
-o jdk-8u151-linux-x64.tar.gz
tar zxf jdk-8u151-linux-x64.tar.gz -C /opt/module
echo 'export JAVA_HOME=/opt/module/jdk1.8.0_151
export PATH=$PATH:$JAVA_HOME/bin' >> ~/.bashrc
source ~/.bashrc
xsync.sh ~/.bashrc
xsync.sh /opt/module/jdk1.8.0_151
xcall.sh java -version
#分发到三台服务器上
```

## 安装Hadoop

```shell script
cd /opt/software
#https://archive.apache.org/dist/hadoop/common/
curl https://archive.apache.org/dist/hadoop/common/hadoop-3.1.4/hadoop-3.1.4.tar.gz \
-o hadoop-3.1.4.tar.gz
tar zxf hadoop-3.1.4.tar.gz -C /opt/module
echo 'export HADOOP_HOME=/opt/module/hadoop-3.1.4
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin' >> ~/.bashrc
source ~/.bashrc
xsync.sh ~/.bashrc
xsync.sh /opt/module/hadoop-3.1.4
```

## 修改Hadoop配置文件

所有的配置文件都在/opt/module/hadoop-3.1.3/etc/hadoop目录下

### core-site.xml

```xml
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>

<configuration>
    <!-- 指定NameNode的地址 -->
    <property>
        <name>fs.defaultFS</name>
        <value>hdfs://hadoop102:9820</value>
    </property>
    <!-- 指定hadoop数据的存储目录 -->
    <property>
        <name>hadoop.tmp.dir</name>
        <value>/opt/module/hadoop-3.1.3/data</value>
    </property>

    <!-- 配置HDFS网页登录使用的静态用户为hadoop -->
    <property>
        <name>hadoop.http.staticuser.user</name>
        <value>hadoop</value>
    </property>

    <!-- 配置该hadoop(superUser)允许通过代理访问的主机节点 -->
    <property>
        <name>hadoop.proxyuser.hadoop.hosts</name>
        <value>*</value>
    </property>
    <!-- 配置该hadoop(superUser)允许通过代理用户所属组 -->
    <property>
        <name>hadoop.proxyuser.hadoop.groups</name>
        <value>*</value>
    </property>
    <!-- 配置该hadoop(superUser)允许通过代理的用户-->
    <property>
        <name>hadoop.proxyuser.hadoop.groups</name>
        <value>*</value>
    </property>

    <!--设置编码器-->
    <property>
        <name>io.compression.codecs</name>
        <value>
            org.apache.hadoop.io.compress.GzipCodec,
            org.apache.hadoop.io.compress.DefaultCodec,
            org.apache.hadoop.io.compress.BZip2Codec,
            org.apache.hadoop.io.compress.SnappyCodec,
            com.hadoop.compression.lzo.LzoCodec,
            com.hadoop.compression.lzo.LzopCodec
        </value>
    </property>

    <!--设置lzo编码器类，需提前上传jar包,jar包可在mvnrepository下载-->
    <!--/opt/module/hadoop-3.1.3/share/hadoop/common/hadoop-lzo-0.4.20.jar-->
    <property>
        <name>io.compression.codec.lzo.class</name>
        <value>com.hadoop.compression.lzo.LzoCodec</value>
    </property>
</configuration>
```

### hdfs-site.xml

```xml
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
    <!-- nn web端访问地址-->
    <property>
        <name>dfs.namenode.http-address</name>
        <value>hadoop102:9870</value>
    </property>

    <!-- 2nn web端访问地址-->
    <property>
        <name>dfs.namenode.secondary.http-address</name>
        <value>hadoop104:9868</value>
    </property>

    <!-- 测试环境指定HDFS副本的数量1 -->
    <!--线上服务器设置3-->
    <property>
        <name>dfs.replication</name>
        <value>1</value>
    </property>
</configuration>

```

### mapred-site.xml

```xml
<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
    <!-- 指定MapReduce程序运行在Yarn上 -->
    <property>
        <name>mapreduce.framework.name</name>
        <value>yarn</value>
    </property>

    <!-- 历史服务器端地址 -->
    <property>
        <name>mapreduce.jobhistory.address</name>
        <value>hadoop102:10020</value>
    </property>

    <!-- 历史服务器web端地址 -->
    <property>
        <name>mapreduce.jobhistory.webapp.address</name>
        <value>hadoop102:19888</value>
    </property>
</configuration>

```

### yarn-site.xml

```xml
<?xml version="1.0"?>
<configuration>
    <!-- 指定MR走shuffle -->
    <property>
        <name>yarn.nodemanager.aux-services</name>
        <value>mapreduce_shuffle</value>
    </property>

    <!-- 指定ResourceManager的地址-->
    <property>
        <name>yarn.resourcemanager.hostname</name>
        <value>hadoop103</value>
    </property>

    <!-- 环境变量的继承 -->
    <property>
        <name>yarn.nodemanager.env-whitelist</name>
        <value>JAVA_HOME,HADOOP_COMMON_HOME,HADOOP_HDFS_HOME,HADOOP_CONF_DIR,CLASSPATH_PREPEND_DISTCACHE,HADOOP_YARN_HOME,HADOOP_MAPRED_HOME</value>
    </property>

    <!-- yarn容器允许分配的最大最小内存 -->
    <property>
        <name>yarn.scheduler.minimum-allocation-mb</name>
        <value>512</value>
    </property>
    <property>
        <name>yarn.scheduler.maximum-allocation-mb</name>
        <value>4096</value>
    </property>

    <!-- yarn容器允许管理的物理内存大小 -->
    <property>
        <name>yarn.nodemanager.resource.memory-mb</name>
        <value>4096</value>
    </property>

    <!-- 关闭yarn对物理内存和虚拟内存的限制检查 -->
    <property>
        <name>yarn.nodemanager.pmem-check-enabled</name>
        <value>false</value>
    </property>
    <property>
        <name>yarn.nodemanager.vmem-check-enabled</name>
        <value>false</value>
    </property>

    <!-- 开启日志聚集功能 -->
    <property>
        <name>yarn.log-aggregation-enable</name>
        <value>true</value>
    </property>

    <!-- 设置日志聚集服务器地址 -->
    <property>
        <name>yarn.log.server.url</name>
        <value>http://hadoop102:19888/jobhistory/logs</value>
    </property>

    <!-- 设置日志保留时间为7天 -->
    <property>
        <name>yarn.log-aggregation.retain-seconds</name>
        <value>604800</value>
    </property>
</configuration>
```

### workers

**这个文件不要出现任何空格，空行**

```
hadoop102
hadoop103
hadoop104
```

## 初始化集群

```shell script
#要在hadoop102上执行，配置了hdfs那台节点
hdfs namenode -format 
```

## 集群启停脚本

```shell script
#!/bin/bash
if [ $# -lt 1 ]
then
    echo "No Args Input..."
    exit ;
fi
case $1 in
"start")
        echo " =================== 启动 hadoop集群 ==================="
        echo " --------------- 启动 hdfs ---------------"
        ssh hadoop102 "/opt/module/hadoop-3.1.3/sbin/start-dfs.sh"
        echo " --------------- 启动 yarn ---------------"
        ssh hadoop103 "/opt/module/hadoop-3.1.3/sbin/start-yarn.sh"
        echo " --------------- 启动 historyserver ---------------"
        ssh hadoop102 "/opt/module/hadoop-3.1.3/bin/mapred --daemon start historyserver"
;;
"stop")
        echo " =================== 关闭 hadoop集群 ==================="
        echo " --------------- 关闭 historyserver ---------------"
        ssh hadoop102 "/opt/module/hadoop-3.1.3/bin/mapred --daemon stop historyserver"
        echo " --------------- 关闭 yarn ---------------"
        ssh hadoop103 "/opt/module/hadoop-3.1.3/sbin/stop-yarn.sh"
        echo " --------------- 关闭 hdfs ---------------"
        ssh hadoop102 "/opt/module/hadoop-3.1.3/sbin/stop-dfs.sh"
;;
*)
    echo "Input Args Error..."
;;
esac
```

## 验证集群

* HDFS：http://hadoop102:9870 
* History：http://hadoop102:19888
* YARN：http://hadoop103:8088
* SNN：http://hadoop104:9868
