#!/usr/bin/env bash

day=$1
if [ -z "$day" ]; then
  day=$(date -d '-1 day' +%F)
fi


app=gmall
sql="
insert overwrite table $app.dwd_dim_sku_info partition (dt = '$day')
select sku.id,
       sku.spu_id,
       spu.spu_name,
       sku.price,
       sku.sku_name,
       sku.sku_desc,
       sku.weight,
       trademark.tm_id,
       trademark.tm_name,
       sku.category3_id,
       c3.category2_id,
       c2.category1_id,
       c3.name,
       c2.name,
       c1.name,
       create_time
from (
         select id,
                spu_id,
                price,
                sku_name,
                sku_desc,
                weight,
                tm_id,
                category3_id,
                create_time
         from $app.ods_sku_info
         where dt = '$day'
     ) sku
         left join (
    select id, spu_name, category3_id, tm_id
    from $app.ods_spu_info
    where dt = '$day'
) spu on sku.spu_id = spu.id
         left join (
    select id, name, category2_id
    from $app.ods_base_category3
    where dt = '$day'
) c3 on c3.id = sku.category3_id
         left join (
    select id, name, category1_id
    from $app.ods_base_category2
    where dt = '$day'
) c2 on c2.id = c3.category2_id
         left join (
    select id, name
    from $app.ods_base_category1
    where dt = '$day'
) c1 on c1.id = c2.category1_id
         left join (
    select tm_id, tm_name
    from $app.ods_base_trademark
    where dt = '$day'
) trademark on trademark.tm_id = sku.tm_id;
"
echo "$sql"
