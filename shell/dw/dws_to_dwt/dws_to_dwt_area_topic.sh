#!/usr/bin/env bash

day=$1
if [ -z "$day" ]; then
  day=$(date -d '-1 day' +%F)
fi

app=gmall
sql="
with new as (
    select id,
           province_name,
           area_code,
           iso_code,
           region_id,
           region_name,
           sum(if(date_format(dt, 'yyyy-MM-dd') = '$day', login_count, 0))      login_count,
           sum(if(date_format(dt, 'yyyy-MM-dd') = '$day', order_count, 0))      order_count,
           sum(if(date_format(dt, 'yyyy-MM-dd') = '$day', order_amount, 0.0))   order_amount,
           sum(if(date_format(dt, 'yyyy-MM-dd') = '$day', payment_count, 0))    payment_count,
           sum(if(date_format(dt, 'yyyy-MM-dd') = '$day', payment_amount, 0.0)) payment_amount,
           sum(login_count)                                                           login_last_30d_count,
           sum(order_count)                                                           order_last_30d_count,
           sum(order_amount)                                                          order_last_30d_amount,
           sum(payment_count)                                                         payment_last_30d_count,
           sum(payment_amount)                                                        payment_last_30d_amount
    from $app.dws_area_stats_daycount
    where dt between date_sub('$day', 30) and '$day'
    group by id, province_name, area_code, iso_code, region_id, region_name
),
     old as (
         select id,
                province_name,
                area_code,
                iso_code,
                region_id,
                region_name,
                login_day_count,
                login_last_30d_count,
                order_day_count,
                order_day_amount,
                order_last_30d_count,
                order_last_30d_amount,
                payment_day_count,
                payment_day_amount,
                payment_last_30d_count,
                payment_last_30d_amount
         from $app.dwt_area_topic
     )
insert
into $app.dwt_area_topic
select nvl(new.id, old.id),
       nvl(new.province_name, old.province_name),
       nvl(new.area_code, old.area_code),
       nvl(new.iso_code, old.iso_code),
       nvl(new.region_id, old.region_id),
       nvl(new.region_name, old.region_name),
       nvl(new.login_count, 0)               login_day_count,
       nvl(new.login_last_30d_count, 0)      login_last_30d_count,
       nvl(new.order_count, 0)               order_day_count,
       nvl(new.order_amount, 0.0)            order_day_amount,
       nvl(new.order_last_30d_count, 0)      order_last_30d_count,
       nvl(new.order_last_30d_amount, 0.0)   order_last_30d_amount,
       nvl(new.payment_count, 0)             payment_day_count,
       nvl(new.payment_amount, 0.0)          payment_day_amount,
       nvl(new.payment_last_30d_count, 0)    payment_last_30d_count,
       nvl(new.payment_last_30d_amount, 0.0) payment_last_30d_amount
from old
         full join new on old.id = new.id;
"
echo "$sql"
