#!/usr/bin/env bash

day=$1
if [ -z "$day" ]; then
  day=$(date -d '-1 day' +%F)
fi

app=gmall
sql="
with login as (
    select mid_id,
           brand,
           model,
           count(*) login_count
    from $app.dwd_start_log
    where dt = '$day'
    group by mid_id, brand, model
),
     page as (
         select mid_id,
                brand,
                model,
                collect_set(named_struct('page_id', page_id, 'page_count', page_count)) page_stats
         from (
                  select mid_id,
                         brand,
                         model,
                         page_id,
                         count(*) page_count
                  from $app.dwd_page_log
                  where dt = '$day'
                  group by mid_id, brand, model, page_id
              ) t
        group by mid_id,
                brand,
                model
     )
insert overwrite table $app.dws_uv_detail_daycount partition(dt = '$day')
select nvl(l.mid_id, p.mid_id),
       nvl(l.brand, p.brand),
       nvl(l.model, p.model),
       login_count,
       page_stats
from login l
         full join page p on l.mid_id = p.mid_id;
"
echo "$sql"
