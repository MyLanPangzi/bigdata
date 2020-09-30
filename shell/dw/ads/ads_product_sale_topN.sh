#!/usr/bin/env bash

day=$1
if [ -z "$day" ]; then
  day=$(date -d '-1 day' +%F)
fi

app=gmall
sql="
insert overwrite table $app.ads_product_sale_topN
select *
from $app.ads_product_sale_topN
union all
select '$day'   dt,
       sku_id,
       payment_amount payment_amount
from $app.dws_sku_action_daycount
where dt = '$day'
order by payment_amount desc
limit 10;
"
echo "$sql"
