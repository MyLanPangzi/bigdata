#!/usr/bin/env bash

day=$2
if [ -z "$day" ]; then
  day=$(date -d '-1 day' +%F)
fi

hive=/opt/module/hive/bin/hive
echo "$hive"

sql=""
cmd="/home/atguigu/bin/ads/${1}.sh"
case $1 in
"all")
  sql="
  $(/home/atguigu/bin/ads/ads_appraise_bad_topN.sh $day)
  $(/home/atguigu/bin/ads/ads_area_topic.sh $day)
  $(/home/atguigu/bin/ads/ads_back_count.sh $day)
  $(/home/atguigu/bin/ads/ads_continuity_uv_count.sh $day)
  $(/home/atguigu/bin/ads/ads_continuity_wk_count.sh $day)
  $(/home/atguigu/bin/ads/ads_new_mid_count.sh $day)
  $(/home/atguigu/bin/ads/ads_order_daycount.sh $day)
  $(/home/atguigu/bin/ads/ads_payment_daycount.sh $day)
  $(/home/atguigu/bin/ads/ads_product_cart_topN.sh $day)
  $(/home/atguigu/bin/ads/ads_product_favor_topN.sh $day)
  $(/home/atguigu/bin/ads/ads_product_info.sh $day)
  $(/home/atguigu/bin/ads/ads_product_refund_topN.sh $day)
  $(/home/atguigu/bin/ads/ads_product_sale_topN.sh $day)
  $(/home/atguigu/bin/ads/ads_sale_tm_category1_stat_mn.sh $day)
  $(/home/atguigu/bin/ads/ads_silent_count.sh $day)
  $(/home/atguigu/bin/ads/ads_user_action_convert_day.sh $day)
  $(/home/atguigu/bin/ads/ads_user_retention_day_rate.sh $day)
  $(/home/atguigu/bin/ads/ads_user_topic.sh $day)
  $(/home/atguigu/bin/ads/ads_uv_count.sh $day)
  "
  ;;
*)
  sql=$($cmd $day)
  ;;
esac
echo "$sql"

hive -e "
SET hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
$sql
"

