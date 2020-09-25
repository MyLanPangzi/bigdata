#!/usr/bin/env bash

day=$1
if [ -z "$day" ]; then
  day=$(date -d '-1 day' +%F)
fi

app=gmall
sql="
with login as (
    select user_id,
           count(*) login_count,
           0        cart_count,
           0        order_count,
           0        order_amount,
           0        payment_count,
           0        payment_amount
    from $app.dwd_start_log
    where dt = '$day'
    group by user_id
),
     cart as (
         select user_id,
                0        login_count,
                count(*) cart_count,
                0        order_count,
                0        order_amount,
                0        payment_count,
                0        payment_amount
         from $app.dwd_action_log
         where dt = '$day'
           and action_item_type = 'cart_add'
         group by user_id
     ),
     \`order\` as (
         select user_id,
                0                                                                                      login_count,
                0                                                                                      cart_count,
                sum(if(date_format(create_time, 'yyyy-MM-dd') = '$day', 1, 0))                   order_count,
                sum(if(date_format(create_time, 'yyyy-MM-dd') = '$day', final_total_amount, 0))  order_amount,
                sum(if(date_format(payment_time, 'yyyy-MM-dd') = '$day', 1, 0))                  payment_count,
                sum(if(date_format(payment_time, 'yyyy-MM-dd') = '$day', final_total_amount, 0)) payment_amount
         from $app.dwd_fact_order_info
         where (dt = '$day' or dt = date_sub('$day', 1))
         group by user_id
     ),
     detail as (
         select user_id,
                collect_set(named_struct('sku_id', sku_id, 'sku_num', sku_num, 'order_count',
                                         order_count, 'order_amount', order_amount)) order_detail_stats
         from (
                  select user_id,
                         sku_id,
                         sum(sku_num)        sku_num,
                         count(*)            order_count,
                         cast(sum(final_amount_d) as decimal(10,2)) order_amount
                  from $app.dwd_fact_order_detail
                  where dt = '$day'
                  group by user_id, sku_id
              ) t
         group by user_id
     )
insert overwrite table $app.dws_user_action_daycount partition (dt='$day')
select nvl(t.user_id, detail.user_id),
       login_count,
       cart_count,
       order_count,
       order_amount,
       payment_count,
       payment_amount,
       order_detail_stats
from (
         select user_id,
                sum(login_count)    login_count,
                sum(cart_count)     cart_count,
                sum(order_count)    order_count,
                sum(order_amount)   order_amount,
                sum(payment_count)  payment_count,
                sum(payment_amount) payment_amount
         from (
                  select *
                  from login
                  union
                  select *
                  from \`order\`
                  union
                  select *
                  from cart
              )t
         group by user_id
     ) t
         full join detail on t.user_id = detail.user_id;
"
echo "$sql"
