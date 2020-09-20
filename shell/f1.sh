#!/usr/bin/env bash

case $1 in
"start")
  echo "开启采集flume"
  for i in hadoop102 hadoop103 ; do
    echo "$i"
    ssh "$i" "nohup flume-ng agent -n a1 -c conf -f /opt/module/flume-1.9.0/conf/file_flume_kafka.conf -Dflume.root.logger=INFO,LOGFILE >/dev/null 2>&1 &"
  done
  ;;
"stop")
  echo "关闭采集flume"
  for i in hadoop102 hadoop103 ; do
    echo "$i"
    ssh "$i" "pgrep -f file_flume_kafka|xargs kill "
  done
esac
