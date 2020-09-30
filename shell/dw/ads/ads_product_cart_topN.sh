#!/usr/bin/env bash

day=$1
if [ -z "$day" ]; then
  day=$(date -d '-1 day' +%F)
fi

app=gmall
sql="
insert overwrite table $app.ads_product_cart_topN
select *
from $app.ads_product_cart_topN
union all
select '$day' dt,
       sku_id,
       cart_count   cart_count
from $app.dws_sku_action_daycount
where dt = '$day'
order by cart_count desc
limit 10;
"
echo "$sql"
