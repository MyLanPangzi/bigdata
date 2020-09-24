#!/usr/bin/env bash

day=$1
if [ -z "$day" ]; then
  day=$(date -d '-1 day' +%F)
fi

app=gmall
sql="
with old as (select mid_id,
                    brand,
                    model,
                    login_first_day,
                    login_last_day,
                    login_day_count,
                    login_count
             from $app.dwt_uv_topic),
     new as (
         select mid_id,
                brand,
                model,
                login_count,
                page_stats
         from $app.dws_uv_detail_daycount
         where dt = '$day'
     )
insert overwrite table $app.dwt_uv_topic
select nvl(new.mid_id, old.mid_id),
       nvl(new.brand, old.brand),
       nvl(new.model, old.model),
       nvl(old.login_first_day, '$day'),
       if(new.mid_id is null, old.login_last_day,'$day'),
       nvl(new.login_count, old.login_count),
       nvl(old.login_count, 0) + if(new.login_count > 0, 1,0)
from old
         full join new   on old.mid_id = new.mid_id;
"
echo "$sql"
