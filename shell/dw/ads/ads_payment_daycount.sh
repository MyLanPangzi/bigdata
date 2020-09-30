#!/usr/bin/env bash

day=$1
if [ -z "$day" ]; then
  day=$(date -d '-1 day' +%F)
fi

app=gmall
sql="
with u as (
    select '$day'                     dt,
           sum(payment_count)               order_count,
           sum(payment_amount)              order_amount,
           sum(if(payment_count > 0, 1, 0)) payment_user_count
    from $app.dwt_user_topic
),
     sku as (
         select '$day' dt,
                count(*)     payment_sku_count
         from $app.dws_sku_action_daycount
         where payment_count > 0
           and dt = '$day'
     ),
     pay_avg as (
         select '$day'                                                         dt,
                avg(unix_timestamp(create_time) - unix_timestamp(payment_time)) / 60 payment_avg_time
         from $app.dwd_fact_order_info
         where payment_time is not null
           and dt = '$day'
     )
insert
overwrite
table
$app.ads_payment_daycount
select *
from $app.ads_payment_daycount
union all
select u.dt,
       order_count,
       order_amount,
       payment_user_count,
       payment_sku_count,
       payment_avg_time
from u
         inner join sku on u.dt = sku.dt
         inner join pay_avg on pay_avg.dt = u.dt;
"
echo "$sql"
