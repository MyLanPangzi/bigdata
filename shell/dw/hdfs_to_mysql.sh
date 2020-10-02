#!/usr/bin/env bash

day=$2
if [ -z "$day" ]; then
  day=$(date -d '-1 day' +%F)
fi

hive=/opt/module/hive/bin/hive
echo "$hive"
sqoop=/opt/module/sqoop/bin/sqoop
hive_db_name=gmall

function export_data() {
  $sqoop export \
  --connect "jdbc:mysql://hadoop102:3306/gmall_report?useSSL=false&useUnicode=true&characterEncoding=utf-8" \
  --username root \
  --password 000000 \
  --input-null-string '\\N' \
  --input-null-non-string '\\N' \
  -m 1 \
  --input-fields-terminated-by '\t' \
  --update-mode allowinsert \
  --export-dir "/warehouse/$hive_db_name/ads/$1" \
  --table $1 \
  --update-key $2
}

case $1 in
  "ads_uv_count")
     export_data "ads_uv_count" "dt"
;;
  "ads_user_action_convert_day")
     export_data "ads_user_action_convert_day" "dt"
;;
  "ads_user_topic")
     export_data "ads_user_topic" "dt"
;;
  "ads_area_topic")
     export_data "ads_area_topic" "dt,iso_code"
;;
   "all")
     export_data "ads_user_topic" "dt"
     export_data "ads_area_topic" "dt,iso_code"
;;
esac
