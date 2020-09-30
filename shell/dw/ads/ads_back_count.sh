#!/usr/bin/env bash

day=$1
if [ -z "$day" ]; then
  day=$(date -d '-1 day' +%F)
fi

app=gmall
sql="
insert overwrite table $app.ads_back_count
select *
from $app.ads_back_count
union all
select '$day'                                                                                         dt,
       concat(date_sub(next_day('$day', 'Monday'), 7), date_sub(next_day('$day', 'Monday'), 1)) wk_dt,
       count(*)                                                                                             wastage_count
from $app.dwt_uv_topic
where login_first_day < date_sub('$day', 7)
  and login_last_day between date_sub(next_day('$day', 'Monday'), 7) and date_sub(next_day('$day', 'Monday'), 1)
  and mid_id not in (
    select mid_id
    from $app.dws_uv_detail_daycount
    where dt between date_sub(next_day('$day', 'Monday'), 14) and date_sub(next_day('$day', 'Monday'), 8)
    group by mid_id
);
"
echo "$sql"
