#!/usr/bin/env bash

case $1 in
"start")
  docker start mysql-hive3
  zkServer.sh start
  kafka-server-start.sh -daemon "$KAFKA_HOME/config/server.properties"
  start-dfs.sh
  start-yarn.sh
  mr-jobhistory-daemon.sh start historyserver
  nohup hive --service metastore >/dev/null 2>&1 &
  nohup hive --service hiveserver2 >/dev/null 2>&1 &

  /opt/module/es7/elasticsearch -d
  /opt/module/pulsar/bin/pulsar-daemon start standalone
# hbase clickhouse
# prometheus grafana
  ;;
"stop")
  ps ef |grep elasticsearch |grep -v grep | awk '{print $2}'  | xargs kill
  /opt/module/pulsar/bin/pulsar-daemon stop standalone

  ps ef |grep metastore |grep -v grep | awk '{print $2}'  | xargs kill
  ps ef |grep hiveserver2 |grep -v grep | awk '{print $2}'  | xargs kill
  mr-jobhistory-daemon.sh stop historyserver
  stop-dfs.sh
  stop-yarn.sh
  kafka-server-stop.sh
  sleep 3
  zkServer.sh stop
  docker stop mysql-hive3
  ;;
esac