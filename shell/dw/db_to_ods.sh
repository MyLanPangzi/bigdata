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
load data inpath '/origin_data/$app/db/activity_order/$day/' overwrite into table $app.ods_activity_order partition (dt='$day');
load data inpath '/origin_data/$app/db/activity_rule/$day/' overwrite into table $app.ods_activity_rule partition (dt='$day');
load data inpath '/origin_data/$app/db/base_category1/$day/' overwrite into table $app.ods_base_category1 partition (dt='$day');
load data inpath '/origin_data/$app/db/base_category2/$day/' overwrite into table $app.ods_base_category2 partition (dt='$day');
load data inpath '/origin_data/$app/db/base_category3/$day/' overwrite into table $app.ods_base_category3 partition (dt='$day');
load data inpath '/origin_data/$app/db/base_dic/$day/' overwrite into table $app.ods_base_dic partition (dt='$day');
load data inpath '/origin_data/$app/db/base_trademark/$day/' overwrite into table $app.ods_base_trademark partition (dt='$day');
load data inpath '/origin_data/$app/db/cart_info/$day/' overwrite into table $app.ods_cart_info partition (dt='$day');
load data inpath '/origin_data/$app/db/comment_info/$day/' overwrite into table $app.ods_comment_info partition (dt='$day');
load data inpath '/origin_data/$app/db/coupon_info/$day/' overwrite into table $app.ods_coupon_info partition (dt='$day');
load data inpath '/origin_data/$app/db/coupon_use/$day/' overwrite into table $app.ods_coupon_use partition (dt='$day');
load data inpath '/origin_data/$app/db/favor_info/$day/' overwrite into table $app.ods_favor_info partition (dt='$day');
load data inpath '/origin_data/$app/db/order_detail/$day/' overwrite into table $app.ods_order_detail partition (dt='$day');
load data inpath '/origin_data/$app/db/order_info/$day/' overwrite into table $app.ods_order_info partition (dt='$day');
load data inpath '/origin_data/$app/db/order_refund_info/$day/' overwrite into table $app.ods_order_refund_info partition (dt='$day');
load data inpath '/origin_data/$app/db/order_status_log/$day/' overwrite into table $app.ods_order_status_log partition (dt='$day');
load data inpath '/origin_data/$app/db/payment_info/$day/' overwrite into table $app.ods_payment_info partition (dt='$day');
load data inpath '/origin_data/$app/db/sku_info/$day/' overwrite into table $app.ods_sku_info partition (dt='$day');
load data inpath '/origin_data/$app/db/spu_info/$day/' overwrite into table $app.ods_spu_info partition (dt='$day');
load data inpath '/origin_data/$app/db/user_info/$day/' overwrite into table $app.ods_user_info partition (dt='$day');
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
