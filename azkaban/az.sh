#!/bin/bash
case $1 in
"start")
  echo "启动azkaban"
  for i in hadoop102 hadoop103 hadoop104 ; do
    echo "$i"
    ssh "$i" "cd /opt/module/azkaban-exec/; bin/start-exec.sh;
        sleep 3;
        curl -G \"$i:\$(<./executor.port)/executor?action=activate\" && echo
      "
  done
  ssh "hadoop102" "cd /opt/module/azkaban-web/; bin/start-web.sh"
  ;;
"stop")
  echo "关闭azkaban"
  ssh "hadoop102" "cd /opt/module/azkaban-web/; bin/shutdown-web.sh"
  for i in hadoop102 hadoop103 hadoop104 ; do
    echo "$i"
      ssh "$i" "cd /opt/module/azkaban-exec/; bin/shutdown-exec.sh"
  done
  ;;
esac

