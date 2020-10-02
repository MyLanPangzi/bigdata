#!/usr/bin/env bash

case $1 in
"start")
  echo "启动presto"
  for i in hadoop102 hadoop103 hadoop104 ; do
    echo "$i"
    ssh "$i" "/opt/module/presto/bin/launcher start"
  done
  ;;
"stop")
  echo "关闭presto"
  for i in hadoop102 hadoop103 hadoop104 ; do
    echo "$i"
    ssh "$i" "/opt/module/presto/bin/launcher stop"
  done
  ;;
esac

