#!/usr/bin/env bash

day=$1
if [ -z "$day" ]; then
  day=$(date -d '-1 day' +%F)
fi

app=gmall
sql="
with pay as (
    select sku.sku_id,
           sum(if(sku.order_count > 0, 1, 0)) buycount,
           sum(if(sku.order_count > 1, 1, 0)) buy_twice_last,
           sum(if(sku.order_count > 2, 1, 0)) buy_3times_last
    from $app.dws_user_action_daycount lateral view explode(order_detail_stats) tmp as sku
    where date_format(dt, 'yyyy-MM') = date_format('$day', 'yyyy-MM')
      and order_detail_stats is not null
    group by sku.sku_id
),
     tm as (
         select tm_id,
                category1_id,
                category1_name,
                buycount,
                buy_twice_last,
                buy_3times_last
         from pay
                  inner join (
             select id,
                    tm_id,
                    category1_id,
                    category1_name
             from $app.dwd_dim_sku_info
             where dt = '$day'
         ) sku on pay.sku_id = sku.id
     )
insert
overwrite
table
$app.ads_sale_tm_category1_stat_mn
select *
from $app.ads_sale_tm_category1_stat_mn
union all
select tm_id,
       category1_id,
       category1_name,
       sum(buycount)                        buycount,
       sum(buy_twice_last)                  buy_twice_last,
       sum(buy_3times_last)                 buy_3times_last,
       sum(buy_twice_last) / sum(buycount)  buy_twice_last_ratio,
       sum(buy_3times_last) / sum(buycount) buy_3times_last_ratio,
       date_format('$day', 'yyyy-MM') stat_mn,
       '$day'                         stat_date
from tm
group by tm_id, category1_id, category1_name;
"
echo "$sql"
