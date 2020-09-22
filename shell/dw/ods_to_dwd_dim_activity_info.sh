#!/usr/bin/env bash

day=$1
if [ -z "$day" ]; then
  day=$(date -d '-1 day' +%F)
fi

app=gmall
sql="
insert overwrite table $app.dwd_dim_activity_info partition (dt='$day')
select id, activity_name, activity_type, start_time, end_time, create_time
from $app.ods_activity_info
where dt = '$day';
"
echo "$sql"
