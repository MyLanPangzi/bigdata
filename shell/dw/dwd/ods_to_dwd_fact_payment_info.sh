#!/usr/bin/env bash

day=$1
if [ -z "$day" ]; then
  day=$(date -d '-1 day' +%F)
fi

app=gmall
sql="
insert overwrite table $app.dwd_fact_payment_info partition (dt='$day')
select p.id,
       out_trade_no,
       order_id,
       user_id,
       alipay_trade_no,
       total_amount,
       subject,
       payment_type,
       payment_time,
       province_id
from (
         select id,
                out_trade_no,
                order_id,
                user_id,
                alipay_trade_no,
                total_amount,
                subject,
                payment_type,
                payment_time
         from $app.ods_payment_info p
         where p.dt = '$day'
     ) p
         inner join (
    select id, province_id
    from $app.ods_order_info
    where dt = '$day'
) o on p.order_id = o.id;
"
echo "$sql"
