#!/usr/bin/env bash

case $1 in
"start")
  nohup hive --service metastore >/dev/null 2>&1 &
  nohup hive --service hiveserver2 >/dev/null 2>&1 &
  ;;
"stop")
  ps ef |grep metastore |grep -v grep | awk '{print $2}'  | xargs kill
  ps ef |grep hiveserver2 |grep -v grep | awk '{print $2}'  | xargs kill
;;
esac
