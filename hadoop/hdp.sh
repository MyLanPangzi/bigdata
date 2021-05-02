#!/usr/bin/env bash

case $1 in
"start")
  zkServer.sh start
  kafka-server-start.sh -daemon "$KAFKA_HOME/config/server.properties"
  start-dfs.sh
  start-yarn.sh
  mr-jobhistory-daemon.sh start historyserver
#  es hbase pulsar mysql clickhouse
# prometheus grafana
  ;;
"stop")
  mr-jobhistory-daemon.sh stop historyserver
  stop-dfs.sh
  stop-yarn.sh
  kafka-server-stop.sh
  sleep 3
  zkServer.sh stop
  ;;
esac