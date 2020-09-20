#!/bin/bash
case $1 in
"start")
  echo "启动kafka"
  for i in hadoop102 hadoop103 hadoop104 ; do
    echo "$i"
    ssh "$i" "kafka-server-start.sh -daemon /opt/module/kafka_2.11-2.4.1/config/server.properties"
  done
  ;;
"stop")
  echo "关闭kafka"
  for i in hadoop102 hadoop103 hadoop104 ; do
    echo "$i"
    ssh "$i" "kafka-server-stop.sh"
  done
  ;;
esac

