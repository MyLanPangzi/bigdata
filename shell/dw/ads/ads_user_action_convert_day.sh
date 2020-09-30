#!/usr/bin/env bash

day=$1
if [ -z "$day" ]; then
  day=$(date -d '-1 day' +%F)
fi

app=gmall
sql="
with page as (
    select '$day'                           dt,
           sum(if(page_id = 'home', 1, 0))        home_count,
           sum(if(page_id = 'good_detail', 1, 0)) good_detail_count
    from $app.dwd_page_log
    where dt = '$day'
),
     u as (
         select '$day'                     dt,
                sum(if(cart_count > 0, 1, 0))    cart_count,
                sum(if(order_count > 0, 1, 0))   order_count,
                sum(if(payment_count > 0, 1, 0)) payment_count
         from $app.dws_user_action_daycount
         where dt = '$day'
     )
insert
overwrite
table
$app.ads_user_action_convert_day
select *
from $app.ads_user_action_convert_day
union all
select u.dt,
       home_count,
       good_detail_count,
       home_count / good_detail_count home2good_detail_convert_ratio,
       cart_count,
       good_detail_count / cart_count good_detail2cart_convert_ratio,
       order_count,
       cart_count / order_count       cart2order_convert_ratio,
       payment_count,
       payment_count / order_count    order2payment_convert_ratio

from page
         inner join u on page.dt = u.dt;
"
echo "$sql"
