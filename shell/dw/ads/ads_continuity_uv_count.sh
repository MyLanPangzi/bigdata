#!/usr/bin/env bash

day=$1
if [ -z "$day" ]; then
  day=$(date -d '-1 day' +%F)
fi

app=gmall
sql="
insert overwrite table $app.ads_continuity_uv_count
select *
from $app.ads_continuity_uv_count
union all
select '$day'                                                                                         dt,
       concat(date_sub(next_day('$day', 'Monday'), 7), date_sub(next_day('$day', 'Monday'), 1)) wk_dt,
       count(*)                                                                                             continuity_count
from (
         select mid_id
         from (
                  select mid_id, dt, datediff(dt, lag(dt, 2, '1970-01-01') over (partition by mid_id order by dt)) diff
                  from $app.dws_uv_detail_daycount
                  where dt between date_sub('$day', 7) and '$day'
              ) t
         where t.diff = 2
         group by mid_id
     ) t;
"
echo "$sql"
