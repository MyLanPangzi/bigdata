#!/usr/bin/env bash

day=$1
if [ -z "$day" ]; then
  day=$(date -d '-1 day' +%F)
fi

app=gmall
sql="
insert overwrite table $app.ads_silent_count
select *
from $app.ads_silent_count
union all
select '$day' dt,
       count(*)     silent_count
from $app.dwt_uv_topic
where login_last_day = login_first_day
  and login_first_day < date_sub('$day', 7);
"
echo "$sql"
