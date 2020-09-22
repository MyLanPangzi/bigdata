#!/usr/bin/env bash

day=$1
if [ -z "$day" ]; then
  day=$(date -d '-1 day' +%F)
fi
#-- 新旧数据合并
#-- 1，查出新数据，关联状态，活动
#-- 2，合并
#-- str_to_map(concat_ws(',', collect_set(concat(order_status, '=', operate_time))), ',', '=')
#-- 拼接字段，聚合数组，数组转换，字符串转map，if判断

app=gmall
sql="
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table $app.dwd_fact_order_info partition (dt)
select if(new.id is null, old.id, new.id),
       if(new.final_total_amount is null, old.final_total_amount, new.final_total_amount),
       if(new.order_status is null, old.order_status, new.order_status),
       if(new.user_id is null, old.user_id, new.user_id),
       if(new.out_trade_no is null, old.out_trade_no, new.out_trade_no),
       if(new.status['1001'] is null, old.create_time, new.status['1001']),--1001对应未支付状态
       if(new.status['1002'] is null, old.payment_time, new.status['1002']),
       if(new.status['1003'] is null, old.cancel_time, new.status['1003']),
       if(new.status['1004'] is null, old.finish_time, new.status['1004']),
       if(new.status['1005'] is null, old.refund_time, new.status['1005']),
       if(new.status['1006'] is null, old.refund_finish_time, new.status['1006']),
       if(new.activity_id is null, old.activity_id, new.activity_id),
       if(new.province_id is null, old.province_id, new.province_id),
       if(new.benefit_reduce_amount is null, old.benefit_reduce_amount, new.benefit_reduce_amount),
       if(new.original_total_amount is null, old.original_total_amount, new.original_total_amount),
       if(new.feight_fee is null, old.feight_fee, new.feight_fee),
       date_format(if(new.create_time is null, old.create_time, new.create_time), 'yyyy-MM-dd')
from (
         select id,
                final_total_amount,
                order_status,
                user_id,
                out_trade_no,
                create_time,
                operate_time,
                province_id,
                benefit_reduce_amount,
                original_total_amount,
                feight_fee,
                activity_id,
                status
         from (
                  select id,
                         final_total_amount,
                         order_status,
                         user_id,
                         out_trade_no,
                         create_time,
                         operate_time,
                         province_id,
                         benefit_reduce_amount,
                         original_total_amount,
                         feight_fee
                  from $app.ods_order_info
                  where dt = '$day'
              ) o
                  left join (
             select activity_id, order_id
             from $app.ods_activity_order
             where dt = '$day'
         ) a on a.order_id = o.id
                  left join (
             select order_id,
                    str_to_map(concat_ws(',', collect_set(concat(order_status, '=', operate_time))), ',', '=') status
             from $app.ods_order_status_log
             where dt = '$day'
             group by order_id
         ) s on o.id = s.order_id
     ) new
         full join (
    select id,
           final_total_amount,
           order_status,
           user_id,
           out_trade_no,
           create_time,
           payment_time,
           cancel_time,
           finish_time,
           refund_time,
           refund_finish_time,
           activity_id,
           province_id,
           benefit_reduce_amount,
           original_total_amount,
           feight_fee
    from $app.dwd_fact_order_info
    where dt in (
        select date_format(create_time, 'yyyy-MM-dd')
        from $app.ods_order_info
        where dt = '$day'
        group by date_format(create_time, 'yyyy-MM-dd')
    )
) old;
"
echo "$sql"
