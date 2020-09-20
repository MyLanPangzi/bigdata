#!/usr/bin/env bash

day=$2
if [ -z "$day" ]; then
    day=$(date -d '-1 day' +%F)
fi
echo "$day"
hive=/opt/module/hive/bin/hive
echo "$hive"
app=gmall

all="
load data inpath '/origin_data/$app/db/activity_info/$day/' overwrite into table $app.ods_activity_info partition (dt='$day');
"
first="
load data inpath '/origin_data/$app/db/base_province/$day/' overwrite into table $app.ods_base_province;
load data inpath '/origin_data/$app/db/base_region/$day/' overwrite into table $app.ods_base_region;
$all
"

case $1 in
"first")
  echo "$first"
  hive -e "$first"
  ;;
"all")
  echo "$all"
  hive -e "$all"
  ;;
esac
