#!/bin/bash
echo "生成埋点日志"
for i in hadoop102 hadoop103; do
	echo "$i"
	ssh $i "cd /opt/module/applog; nohup java -jar gmall2020-mock-log-2020-04-01.jar >/dev/null 2>&1 &"
done
