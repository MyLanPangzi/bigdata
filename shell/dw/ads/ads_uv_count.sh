#!/usr/bin/env bash

day=$1
if [ -z "$day" ]; then
  day=$(date -d '-1 day' +%F)
fi

app=gmall
sql="
insert overwrite table $app.ads_uv_count
select *
from $app.ads_uv_count
union all
select '$day',
       t.day_count,
       t.wk_count,
       t.mn_count,
       if('$day' = date_sub(next_day('$day', 'Monday'), 1), 'Y', 'N') is_weekend,
       if('$day' = last_day('$day'), 'Y', 'N')                        is_monthend

from (
         select sum(if(date_format(dt, 'yyyy-MM-dd') = '$day', 1, 0))                      day_count,
                sum(if(
                        dt between date_sub(next_day('$day', 'Monday'), 7) and date_sub(next_day('$day', 'Monday'), 1),
                        1, 0))                                                                   wk_count,
                sum(if(date_format(dt, 'yyyy-MM') = date_format('$day', 'yyyy-MM'), 1, 0)) mn_count
         from $app.dws_uv_detail_daycount
         where dt between date_format('$day', 'yyyy-MM-01') and last_day('$day')
     ) t;
"
echo "$sql"
