#!/bin/bash

echo  "操作zk $1"
for i in hadoop102 hadoop103 hadoop104 ; do 
  ssh "$i" "zkServer.sh $1"
done 
