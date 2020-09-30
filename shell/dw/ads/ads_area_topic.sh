#!/usr/bin/env bash

day=$1
if [ -z "$day" ]; then
  day=$(date -d '-1 day' +%F)
fi

app=gmall
sql="
insert overwrite table $app.ads_area_topic
select *
from $app.ads_area_topic
union all
select '$day' dt,
       id,
       province_name,
       area_code,
       iso_code,
       region_id,
       region_name,
       cast(login_day_count as bigint),
       order_day_count,
       order_day_amount,
       payment_day_count,
       payment_day_amount
from $app.dwt_area_topic;
"
echo "$sql"
