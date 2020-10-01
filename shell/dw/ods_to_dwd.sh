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
"dwd_dim_base_province")
  sql=$(/home/atguigu/bin/ods_to_dwd_dim_base_province.sh)
  ;;
"dwd_fact_payment_info")
  sql=$(/home/atguigu/bin/ods_to_dwd_fact_payment_info.sh $day)
  ;;
"dwd_fact_refund_info")
  sql=$(/home/atguigu/bin/ods_to_dwd_fact_refund_info.sh $day)
  ;;
"dwd_fact_comment_info")
  sql=$(/home/atguigu/bin/ods_to_dwd_fact_comment_info.sh $day)
  ;;
"dwd_fact_cart_info")
  sql=$(/home/atguigu/bin/ods_to_dwd_fact_cart_info.sh $day)
  ;;
"dwd_fact_favor_info")
  sql=$(/home/atguigu/bin/ods_to_dwd_fact_favor_info.sh $day)
  ;;
"dwd_fact_order_detail")
  sql=$(/home/atguigu/bin/ods_to_dwd_fact_order_detail.sh $day)
  ;;
"dwd_fact_coupon_use")
  sql=$(/home/atguigu/bin/ods_to_dwd_fact_coupon_use.sh $day)
  ;;
"dwd_fact_order_info")
  sql=$(/home/atguigu/bin/ods_to_dwd_fact_order_info.sh $day)
  ;;
"dwd_dim_user_info")
  sql=$(/home/atguigu/bin/ods_to_dwd_dim_user_info.sh $day)
  ;;
"first")
  sql="
  $(/home/atguigu/bin/ods_to_dwd_dim_base_province.sh)
  $(/home/atguigu/bin/ods_to_dwd_dim_sku.sh $day)
  $(/home/atguigu/bin/ods_to_dwd_dim_coupon.sh $day)
  $(/home/atguigu/bin/ods_to_dwd_dim_activity_info.sh $day)
  $(/home/atguigu/bin/ods_to_dwd_fact_payment_info.sh $day)
  $(/home/atguigu/bin/ods_to_dwd_fact_refund_info.sh $day)
  $(/home/atguigu/bin/ods_to_dwd_fact_comment_info.sh $day)
  $(/home/atguigu/bin/ods_to_dwd_fact_cart_info.sh $day)
  $(/home/atguigu/bin/ods_to_dwd_fact_favor_info.sh $day)
  $(/home/atguigu/bin/ods_to_dwd_fact_order_detail.sh $day)
  $(/home/atguigu/bin/ods_to_dwd_fact_coupon_use.sh $day)
  $(/home/atguigu/bin/ods_to_dwd_fact_order_info.sh $day)
  $(/home/atguigu/bin/ods_to_dwd_dim_user_info.sh $day)
  "
  ;;
"all")
  sql="
  $(/home/atguigu/bin/ods_to_dwd_dim_sku.sh $day)
  $(/home/atguigu/bin/ods_to_dwd_dim_coupon.sh $day)
  $(/home/atguigu/bin/ods_to_dwd_dim_activity_info.sh $day)
  $(/home/atguigu/bin/ods_to_dwd_fact_payment_info.sh $day)
  $(/home/atguigu/bin/ods_to_dwd_fact_refund_info.sh $day)
  $(/home/atguigu/bin/ods_to_dwd_fact_comment_info.sh $day)
  $(/home/atguigu/bin/ods_to_dwd_fact_cart_info.sh $day)
  $(/home/atguigu/bin/ods_to_dwd_fact_favor_info.sh $day)
  $(/home/atguigu/bin/ods_to_dwd_fact_order_detail.sh $day)
  $(/home/atguigu/bin/ods_to_dwd_fact_coupon_use.sh $day)
  $(/home/atguigu/bin/ods_to_dwd_fact_order_info.sh $day)
  $(/home/atguigu/bin/ods_to_dwd_dim_user_info.sh $day)
  "
  ;;
"log")
  sql="
   $(/home/atguigu/bin/ods_to_dwd_err_log.sh $day)
   $(/home/atguigu/bin/ods_to_dwd_action_log.sh $day)
   $(/home/atguigu/bin/ods_to_dwd_display_log.sh $day)
   $(/home/atguigu/bin/ods_to_dwd_page_log.sh $day)
   $(/home/atguigu/bin/ods_to_dwd_start_log.sh $day)
  ;;
esac

echo "$sql"

hive -e "
SET hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
$sql
"
