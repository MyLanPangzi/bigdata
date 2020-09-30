#!/usr/bin/env bash

day=$1
if [ -z "$day" ]; then
  day=$(date -d '-1 day' +%F)
fi

app=gmall
sql="
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table $app.dwd_fact_coupon_use partition (dt)
select if(new.id is null, old.id, new.id),
       if(new.coupon_id is null, old.coupon_id, new.coupon_id),
       if(new.user_id is null, old.user_id, new.user_id),
       if(new.order_id is null, old.order_id, new.order_id),
       if(new.coupon_status is null, old.coupon_status, new.coupon_status),
       if(new.get_time is null, old.get_time, new.get_time),
       if(new.using_time is null, old.using_time, new.using_time),
       if(new.used_time is null, old.used_time, new.used_time),
       date_format(if(new.get_time is null, old.get_time, new.get_time), 'yyyy-MM-dd')
from (
         select id,
                coupon_id,
                user_id,
                order_id,
                coupon_status,
                get_time,
                using_time,
                used_time
         from $app.ods_coupon_use
         where dt = '$day'
     ) old
         full join (
    select id,
           coupon_id,
           user_id,
           order_id,
           coupon_status,
           get_time,
           using_time,
           used_time
    from $app.dwd_fact_coupon_use
    where dt in (
        select date_format(get_time, 'yyyy-MM-dd')
        from $app.ods_coupon_use
        where dt = '$day'
        group by date_format(get_time, 'yyyy-MM-dd')
    )
) new;
"
echo "$sql"
