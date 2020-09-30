#!/usr/bin/env bash

day=$1
if [ -z "$day" ]; then
  day=$(date -d '-1 day' +%F)
fi

app=gmall
sql="
with sku as (
    select '$day' dt,
           count(*)     sku_num
    from $app.dwt_sku_topic
),
     spu as (
         select '$day' dt,
                count(*)     spu_num
         from (
                  select spu_id
                  from $app.dwt_sku_topic
                  group by spu_id
              ) t
     )
insert
overwrite
table
$app.ads_product_info
select *
from $app.ads_product_info
union all
select '$day' dt,
       sku_num,
       spu_num
from sku
         inner join spu on sku.dt = spu.dt;
"
echo "$sql"
