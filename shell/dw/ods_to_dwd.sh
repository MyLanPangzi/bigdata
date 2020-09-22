#!/usr/bin/env bash

day=$2
if [ -z "$day" ]; then
  day=$(date -d '-1 day' +%F)
fi

hive=/opt/module/hive/bin/hive
echo "$hive"

sql=""
case $1 in
"dwd_err_log")
  sql=$(/home/atguigu/bin/ods_to_dwd_err_log.sh $day)
  ;;
"dwd_action_log")
  sql=$(/home/atguigu/bin/ods_to_dwd_action_log.sh $day)
  ;;
"dwd_display_log")
  sql=$(/home/atguigu/bin/ods_to_dwd_display_log.sh $day)
  ;;
"dwd_page_log")
  sql=$(/home/atguigu/bin/ods_to_dwd_page_log.sh $day)
  ;;
"dwd_start_log")
  sql=$(/home/atguigu/bin/ods_to_dwd_start_log.sh $day)
  ;;
"dwd_dim_sku_info")
  sql=$(/home/atguigu/bin/ods_to_dwd_dim_sku.sh $day)
  ;;
"dwd_dim_coupon_info")
  sql=$(/home/atguigu/bin/ods_to_dwd_dim_coupon.sh $day)
  ;;
"dwd_dim_activity_info")
  sql=$(/home/atguigu/bin/ods_to_dwd_dim_activity_info.sh $day)
  ;;
esac

echo "$sql"

hive -e "
SET hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
$sql
"
