#!/usr/bin/env bash

day=$1
if [ -z "$day" ]; then
  day=$(date -d '-1 day' +%F)
fi

app=gmall
sql="
insert overwrite table $app.dwd_fact_favor_info partition (dt='$day')
select id,
       user_id,
       sku_id,
       spu_id,
       is_cancel,
       create_time,
       cancel_time
from $app.ods_favor_info
where dt = '$day';
"
echo "$sql"
