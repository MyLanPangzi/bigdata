#!/usr/bin/env bash

day=$1
if [ -z "$day" ]; then
  day=$(date -d '-1 day' +%F)
fi

app=gmall
sql="
with one as (
    select mid_id
    from $app.dws_uv_detail_daycount
    where dt between date_sub(next_day('$day', 'Monday'), 7) and date_sub(next_day('$day', 'Monday'), 1)
    group by mid_id
),
     two as (
         select mid_id
         from $app.dws_uv_detail_daycount
         where dt between date_sub(next_day('$day', 'Monday'), 7 * 2) and date_sub(next_day('$day', 'Monday'), 7 + 1)
         group by mid_id
     ),
     three as (
         select mid_id
         from $app.dws_uv_detail_daycount
         where dt between date_sub(next_day('$day', 'Monday'), 7 * 3) and date_sub(next_day('$day', 'Monday'), 7 * 2 + 1)
         group by mid_id
     )
insert
overwrite
table
$app.ads_continuity_wk_count
select *
from $app.ads_continuity_wk_count
union all
select '$day'                                                                                         dt,
       concat(date_sub(next_day('$day', 'Monday'), 7), date_sub(next_day('$day', 'Monday'), 1)) wk_dt,
       count(*)                                                                                             continuity_count
from one
         inner join two on one.mid_id = two.mid_id
         inner join three on one.mid_id = three.mid_id;
"
echo "$sql"
