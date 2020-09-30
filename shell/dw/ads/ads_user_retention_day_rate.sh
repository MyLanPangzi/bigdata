#!/usr/bin/env bash

day=$1
if [ -z "$day" ]; then
  day=$(date -d '-1 day' +%F)
fi

app=gmall
sql="
insert overwrite table $app.ads_user_retention_day_rate
select *
from $app.ads_user_retention_day_rate
union all
select '$day'                                                                                 stat_date,
       date_format(date_sub('$day', 1), 'yyyy-MM-dd')                                         create_date,
       1                                                                                            retention_day,
       sum(if(login_first_day = date_sub('$day', 1) and login_last_day = '$day', 1, 0)) retention_count,
       sum(if(login_first_day = '$day', 1, 0))                                                new_mid_count,
       sum(if(login_first_day = date_sub('$day', 1), 1, 0)) /
       sum(if(login_first_day = '$day', 1, 0)) * 100                                          retention_ratio
from $app.dwt_uv_topic
union all
select '$day'                                                                                 stat_date,
       date_format(date_sub('$day', 2), 'yyyy-MM-dd')                                         create_date,
       2                                                                                            retention_day,
       sum(if(login_first_day = date_sub('$day', 2) and login_last_day = '$day', 1, 0)) retention_count,
       sum(if(login_first_day = '$day', 1, 0))                                                new_mid_count,
       sum(if(login_first_day = date_sub('$day', 2), 1, 0)) /
       sum(if(login_first_day = '$day', 1, 0)) * 100                                          retention_ratio
from $app.dwt_uv_topic
union all
select '$day'                                                                                 stat_date,
       date_format(date_sub('$day', 3), 'yyyy-MM-dd')                                         create_date,
       3                                                                                            retention_day,
       sum(if(login_first_day = date_sub('$day', 3) and login_last_day = '$day', 1, 0)) retention_count,
       sum(if(login_first_day = '$day', 3, 0))                                                new_mid_count,
       sum(if(login_first_day = date_sub('$day', 3), 1, 0)) /
       sum(if(login_first_day = '$day', 1, 0)) * 100                                          retention_ratio
from $app.dwt_uv_topic;

"
echo "$sql"
