#!/usr/bin/env bash

day=$1
if [ -z "$day" ]; then
  day=$(date -d '-1 day' +%F)
fi

app=gmall
sql="
insert overwrite table $app.dwd_fact_cart_info partition (dt='$day')
select id,
       user_id,
       sku_id,
       cart_price,
       sku_num,
       sku_name,
       create_time,
       operate_time,
       is_ordered,
       order_time,
       source_type,
       source_id
from $app.ods_cart_info
where dt='$day';"
echo "$sql"
