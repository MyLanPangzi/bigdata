# .bashrc

# Source global definitions
if [ -f /etc/bashrc ]; then
	. /etc/bashrc
fi

# Uncomment the following line if you don't like systemctl's auto-paging feature:
# export SYSTEMD_PAGER=

# User specific aliases and functions
export JAVA_HOME=/opt/module/jdk1.8.0_212
export HADOOP_HOME=/opt/module/hadoop-3.1.3
export ZOOKEEPER_HOME=/opt/module/zookeeper-3.5.7
export KAFKA_HOME=/opt/module/kafka_2.11-2.4.1
export FLUME_HOME=/opt/module/flume-1.9.0
export SQOOP_HOME=/opt/module/sqoop
export HIVE_HOME=/opt/module/hive
export PATH=$PATH:$SQOOP_HOME/bin:$HIVE_HOME/bin
export PATH=$PATH:$JAVA_HOME/bin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$ZOOKEEPER_HOME/bin:$KAFKA_HOME/bin:$FLUME_HOME/bin
export SPARK_HOME=/opt/module/spark
export PATH=$PATH:$SPARK_HOME/bin
