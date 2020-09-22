#!/usr/bin/env bash

day=$1
if [ -z "$day" ]; then
  day=$(date -d '-1 day' +%F)
fi


app=gmall
sql="
SET hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table $app.dwd_dim_coupon_info partition (dt = '$day')
select id,
       coupon_name,
       coupon_type,
       condition_amount,
       condition_num,
       activity_id,
       benefit_amount,
       benefit_discount,
       create_time,
       range_type,
       spu_id,
       tm_id,
       category3_id,
       limit_num,
       operate_time,
       expire_time
from $app.ods_coupon_info
where dt = '$day';
"
echo "$sql"
