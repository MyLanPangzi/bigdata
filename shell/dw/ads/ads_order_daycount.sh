#!/usr/bin/env bash

day=$1
if [ -z "$day" ]; then
  day=$(date -d '-1 day' +%F)
fi

app=gmall
sql="
insert overwrite table $app.ads_order_daycount
select *
from $app.ads_order_daycount
union all
select '$day'                   dt,
       sum(order_count)               order_count,
       sum(order_amount)              order_amount,
       sum(if(order_count > 0, 1, 0)) order_users
from $app.dwt_user_topic;
"
echo "$sql"
