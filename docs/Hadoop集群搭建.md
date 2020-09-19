# Hadoop集群搭建

## 步骤描述

* 配置前提条件
* 安装jdk，分发
* 安装hadoop，分发
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

### core-site.xml

### hdfs-site.xml

### mapred-site.xml

### yarn-site.xml
