#!/usr/bin/env bash

day=$1
if [ -z "$day" ]; then
  day=$(date -d '-1 day' +%F)
fi
app=gmall
sql="
insert overwrite table $app.dws_activity_info_daycount partition (dt = '$day')
select id,
       activity_name,
       activity_type,
       start_time,
       end_time,
       create_time,
       sum(display_count),
       sum(order_count),
       sum(order_amount),
       sum(payment_count),
       sum(payment_amount)
from (
         select display_item activity_id,
                count(*)     display_count,
                0            order_count,
                0            order_amount,
                0            payment_count,
                0            payment_amount
         from $app.dwd_display_log
         where display_item_type = 'activity_id'
           and dt = '$day'
         group by display_item
         union
         select activity_id,
                0                                                                                      display_count,
                sum(if(date_format(create_time, 'yyyy-MM-dd') = '$day', 1, 0))                   order_count,
                sum(if(date_format(create_time, 'yyyy-MM-dd') = '$day', final_total_amount, 0))  order_amount,
                sum(if(date_format(payment_time, 'yyyy-MM-dd') = '$day', 1, 0))                  payment_count,
                sum(if(date_format(payment_time, 'yyyy-MM-dd') = '$day', final_total_amount, 0)) payment_amount
         from $app.dwd_fact_order_info
         where (dt = '$day' or date_sub('$day', 1) = dt)
           and activity_id is not null
         group by activity_id
     ) t
         left join (
    select id, activity_name, activity_type, start_time, end_time, create_time
    from $app.dwd_dim_activity_info
    where dt = '$day'
) a on a.id = t.activity_id
group by id, activity_name, activity_type, start_time, end_time, create_time;
"
echo "$sql"
