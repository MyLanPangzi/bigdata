#!/usr/bin/env bash

case $1 in
"start")
  echo "开启消费flume hadoop104"
  ssh "hadoop104" "nohup flume-ng agent -n a1 -c conf -f /opt/module/flume-1.9.0/conf/kafka_file_hdfs.conf -Dflume.root.logger=INFO,LOGFILE >//opt/module/flume-1.9.0/f2.log 2>&1 &"
  ;;
"stop")
  echo "关闭消费flume hadoop104"
  ssh "hadoop104" "pgrep -f kafka_file_hdfs|xargs kill "
  ;;
esac
