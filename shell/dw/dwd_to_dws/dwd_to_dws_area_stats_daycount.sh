#!/usr/bin/env bash

day=$1
if [ -z "$day" ]; then
  day=$(date -d '-1 day' +%F)
fi
app=gmall
sql="
with login as (
    select area_code,
           count(*) login_count
    from $app.dwd_start_log d
    where dt = '$day'
    group by area_code
),
     pay as (
         select province_id,
                sum(if(date_format(create_time, 'yyyy-MM-dd') = '$day', 1, 0))  order_count,
                sum(if(date_format(create_time, 'yyyy-MM-dd') = '$day', final_total_amount,
                       0))                                                            order_amount,
                sum(if(date_format(payment_time, 'yyyy-MM-dd') = '$day', 1, 0)) payment_count,
                sum(if(date_format(payment_time, 'yyyy-MM-dd') = '$day', final_total_amount,
                       0))                                                            payment_amount
         from $app.dwd_fact_order_info
         where (dt = '$day' or date_sub('$day', 1) = dt)
         group by province_id
     )
insert
overwrite
table
$app.dws_area_stats_daycount
partition
(
dt = '$day'
)
select id,
       name,
       p.area_code,
       iso_code,
       region_id,
       region_name,
       nvl(login_count, 0),
       nvl(order_count, 0),
       nvl(order_amount, 0),
       nvl(payment_count, 0),
       nvl(payment_amount, 0)
from $app.dwd_dim_base_province p
         left join login l on l.area_code = p.area_code
         left join pay on pay.province_id = p.id;
"
echo "$sql"
