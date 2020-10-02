#!/usr/bin/env bash
for i in hadoop102 hadoop103 hadoop104; do
  echo "$i"
  ssh "$i" "echo \"
      node.environment=production
      node.id=$i
      node.data-dir=/opt/module/presto/data\" > /opt/module/presto/etc/node.properties"
done
