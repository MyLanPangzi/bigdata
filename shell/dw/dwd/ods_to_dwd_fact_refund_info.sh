#!/usr/bin/env bash

day=$1
if [ -z "$day" ]; then
  day=$(date -d '-1 day' +%F)
fi

app=gmall
sql="
insert overwrite table $app.dwd_fact_refund_info partition (dt = '$day')
select id,
       user_id,
       order_id,
       sku_id,
       refund_type,
       refund_num,
       refund_amount,
       refund_reason_type,
       create_time
from $app.ods_order_refund_info
where dt = '$day';
"
echo "$sql"
