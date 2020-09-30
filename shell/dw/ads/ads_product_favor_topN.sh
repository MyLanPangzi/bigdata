#!/usr/bin/env bash

day=$1
if [ -z "$day" ]; then
  day=$(date -d '-1 day' +%F)
fi

app=gmall
sql="
insert overwrite table $app.ads_product_favor_topN
select *
from $app.ads_product_favor_topN
union all
select '$day' dt,
       sku_id,
       favor_count  favor_count
from $app.dws_sku_action_daycount
where dt = '$day'
order by favor_count desc
limit 10;
"
echo "$sql"
