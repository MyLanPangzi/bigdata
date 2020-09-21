#!/usr/bin/env bash

day=$1
if [ -z "$day" ]; then
    day=$(date -d '-1 day' +%F)
fi
echo "$day"

hive=/opt/module/hive/bin/hive
echo "$hive"

app=gmall
hive -e "load data inpath '/origin_data/$app/log/topic_log/$day/' overwrite into table $app.ods_log partition (dt='$day');"

hadoop=/opt/module/hadoop-3.1.3/bin/hadoop
$hadoop jar /opt/module/hadoop-3.1.3/share/hadoop/common/hadoop-lzo-0.4.20.jar \
com.hadoop.compression.lzo.DistributedLzoIndexer \
-Dmapreduce.job.queuename=hive \
/warehouse/$app/ods/ods_log/dt=$day