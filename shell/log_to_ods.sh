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
