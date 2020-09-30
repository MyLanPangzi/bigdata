#!/usr/bin/env bash

day=$1
if [ -z "$day" ]; then
  day=$(date -d '-1 day' +%F)
fi

app=gmall
sql="
insert overwrite table $app.ads_product_refund_topN
select *
from $app.ads_product_refund_topN
union all
select '$day'                 dt,
       sku_id,
       refund_count / payment_count refund_ratio
from $app.dws_sku_action_daycount
where dt between date_sub('$day', 30) and '$day'
order by refund_ratio desc
limit 10;
"
echo "$sql"
