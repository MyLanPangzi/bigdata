#!/usr/bin/env bash

case $1 in
"start")
  zk.sh start
  sleep 3
  hdp.sh start
  kf.sh start
  f2.sh start
  f1.sh start
  hs.sh start
  ;;
"stop")
  hs.sh stop
  f1.sh stop
  f2.sh stop
  kf.sh stop
  hdp.sh stop
  sleep 3
  zk.sh stop
  ;;
esac