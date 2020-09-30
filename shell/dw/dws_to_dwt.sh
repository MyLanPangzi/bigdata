#!/usr/bin/env bash

day=$2
if [ -z "$day" ]; then
  day=$(date -d '-1 day' +%F)
fi

hive=/opt/module/hive/bin/hive
echo "$hive"

sql=""
case $1 in
"dwt_uv_topic")
  sql=$(/home/atguigu/bin/dws_to_dwt_uv_topic.sh $day)
  ;;
"dwt_user_topic")
  sql=$(/home/atguigu/bin/dws_to_dwt_user_topic.sh $day)
  ;;
"dwt_sku_topic")
  sql=$(/home/atguigu/bin/dws_to_dwt_sku_topic.sh $day)
  ;;
"dwt_area_topic")
  sql=$(/home/atguigu/bin/dws_to_dwt_sku_topic.sh $day)
  ;;
"dwt_activity_topic")
  sql=$(/home/atguigu/bin/dws_to_dwt_sku_topic.sh $day)
  ;;
esac

echo "$sql"

hive -e "
$sql
"

