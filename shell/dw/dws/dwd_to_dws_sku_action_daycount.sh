#!/usr/bin/env bash

day=$1
if [ -z "$day" ]; then
  day=$(date -d '-1 day' +%F)
fi

sql="
with detail as (
    select sku_id,
           sum(if(date_format(create_time, 'yyyy-MM-dd') = '$day', 1, 0))               order_count,
           sum(if(date_format(create_time, 'yyyy-MM-dd') = '$day', sku_num, 0))         order_num,
           sum(if(date_format(create_time, 'yyyy-MM-dd') = '$day', final_amount_d, 0))  order_amount,
           sum(if(date_format(payment_time, 'yyyy-MM-dd') = '$day', 1, 0))              payment_count,
           sum(if(date_format(payment_time, 'yyyy-MM-dd') = '$day', sku_num, 0))        payment_num,
           sum(if(date_format(payment_time, 'yyyy-MM-dd') = '$day', final_amount_d, 0)) payment_amount,
           sum(if(date_format(refund_time, 'yyyy-MM-dd') = '$day', 1, 0))               refund_count,
           sum(if(date_format(refund_time, 'yyyy-MM-dd') = '$day', sku_num, 0))         refund_num,
           sum(if(date_format(refund_time, 'yyyy-MM-dd') = '$day', final_amount_d, 0))  refund_amount,
           0                                                                            cart_count,
           0                                                                            favor_count,
           0                                                                            appraise_good_count,
           0                                                                            appraise_mid_count,
           0                                                                            appraise_bad_count,
           0                                                                            appraise_default_count
    from (
             select id, create_time, payment_time, refund_time
             from gmall.dwd_fact_order_info
             where dt = '$day'
                or dt = date_sub('$day', 1)
         ) o
             left join (
        select order_id, sku_id, sku_num, final_amount_d
        from gmall.dwd_fact_order_detail
        where dt = '$day'
           or dt = date_sub('$day', 1)
    ) d on d.order_id = o.id
    group by sku_id
),
     cart as (
         select action_item sku_id,
                0           order_count,
                0           order_num,
                0           order_amount,
                0           payment_count,
                0           payment_num,
                0           payment_amount,
                0           refund_count,
                0           refund_num,
                0           refund_amount,
                count(*)    cart_count,
                0           favor_count,
                0           appraise_good_count,
                0           appraise_mid_count,
                0           appraise_bad_count,
                0           appraise_default_count
         from gmall.dwd_action_log
         where action_item_type = 'cart_add'
           and dt = '$day'
         group by action_item
     ),
     comment as (
         select sku_id,
                0                                order_count,
                0                                order_num,
                0                                order_amount,
                0                                payment_count,
                0                                payment_num,
                0                                payment_amount,
                0                                refund_count,
                0                                refund_num,
                0                                refund_amount,
                0                                cart_count,
                0                                favor_count,
                sum(if(appraise = '1201', 1, 0)) appraise_good_count,
                sum(if(appraise = '1202', 1, 0)) appraise_mid_count,
                sum(if(appraise = '1203', 1, 0)) appraise_bad_count,
                sum(if(appraise = '1204', 1, 0)) appraise_default_count
         from gmall.dwd_fact_comment_info
         where dt = '$day'
         group by sku_id
     )
insert
overwrite
table
gmall.dws_sku_action_daycount
partition
(
dt = '$day'
)
select sku_id,
       sum(order_count),
       sum(order_num),
       sum(order_amount),
       sum(payment_count),
       sum(payment_num),
       sum(payment_amount),
       sum(refund_count),
       sum(refund_num),
       sum(refund_amount),
       sum(cart_count),
       sum(favor_count),
       sum(appraise_good_count),
       sum(appraise_mid_count),
       sum(appraise_bad_count),
       sum(appraise_default_count)
from (
         select *
         from detail
         union
         select *
         from cart
         union
         select *
         from comment
     )t
group by sku_id;
"
echo "$sql"
