#!/usr/bin/env bash
for i in hadoop103 hadoop104; do
  echo "$i"
  ssh "$i" "echo \"
coordinator=false
http-server.http.port=8881
query.max-memory=50GB
discovery.uri=http://hadoop102:8881\" > /opt/module/presto/etc/config.properties"

done
