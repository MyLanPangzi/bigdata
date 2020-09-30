#!/usr/bin/env bash

day=$1
if [ -z "$day" ]; then
  day=$(date -d '-1 day' +%F)
fi

app=gmall
sql="
with new as (
    select id,
           activity_name,
           activity_type,
           start_time,
           end_time,
           create_time,
           display_count,
           order_count,
           order_amount,
           payment_count,
           payment_amount
    from $app.dws_activity_info_daycount
    where dt = '$day'
),
     old as (
         select id,
                activity_name,
                activity_type,
                start_time,
                end_time,
                create_time,
                display_day_count,
                order_day_count,
                order_day_amount,
                payment_day_count,
                payment_day_amount,
                display_count,
                order_count,
                order_amount,
                payment_count,
                payment_amount
         from $app.dwt_activity_topic
     )
insert
into $app.dwt_activity_topic
select nvl(new.id, old.id),
       nvl(new.activity_name, old.activity_name),
       nvl(new.activity_type, old.activity_type),
       nvl(new.start_time, old.start_time),
       nvl(new.end_time, old.end_time),
       nvl(new.create_time, old.create_time),
       nvl(new.display_count, 0),
       nvl(new.order_count, 0),
       nvl(new.order_amount, 0),
       nvl(new.payment_count, 0),
       nvl(new.payment_amount, 0),
       nvl(old.order_count, 0) + nvl(new.display_count, 0),
       nvl(old.order_count, 0) + nvl(new.order_count, 0),
       nvl(old.order_amount, 0) + nvl(new.order_amount, 0),
       nvl(old.payment_count, 0) + nvl(new.payment_count, 0),
       nvl(old.payment_amount, 0) + nvl(new.payment_amount, 0)
from old
         full join new on new.id = old.id;
"
echo "$sql"
