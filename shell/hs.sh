#!/usr/bin/env bash
case $1 in
"start")
  nohup hiveserver2 > /tmp/atguigu/hiveserver2.log 2>&1 &
  ;;
"stop")
  pgrep -f hiveserver2 |xargs kill
;;
esac
