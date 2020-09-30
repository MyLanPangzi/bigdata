#!/usr/bin/env bash

day=$1
if [ -z "$day" ]; then
  day=$(date -d '-1 day' +%F)
fi

app=gmall
sql="
insert overwrite table $app.ads_new_mid_count
select *
from $app.ads_new_mid_count
union all
select '$day',
       count(*)
from $app.dwt_uv_topic
where login_last_day = login_first_day
  and login_first_day = '$day';
"
echo "$sql"
