#!/usr/bin/env bash

day=$1
if [ -z "$day" ]; then
  day=$(date -d '-1 day' +%F)
fi

app=gmall
sql="
insert into table $app.dwt_sku_topic
select nvl(new.sku_id, old.sku_id),
       sku.spu_id,
       nvl(new.order_last_30d_count, 0),
       nvl(new.order_last_30d_num, 0),
       nvl(new.order_last_30d_amount, 0),
       nvl(old.order_count, 0) + nvl(new.order_count, 0),
       nvl(old.order_num, 0) + nvl(new.order_num, 0),
       nvl(old.order_amount, 0) + nvl(new.order_amount, 0),
       nvl(new.payment_last_30d_count, 0),
       nvl(new.payment_last_30d_num, 0),
       nvl(new.payment_last_30d_amount, 0),
       nvl(old.payment_count, 0) + nvl(new.payment_count, 0),
       nvl(old.payment_num, 0) + nvl(new.payment_num, 0),
       nvl(old.payment_amount, 0) + nvl(new.payment_amount, 0),
       nvl(new.refund_last_30d_count, 0),
       nvl(new.refund_last_30d_num, 0),
       nvl(new.refund_last_30d_amount, 0),
       nvl(old.refund_count, 0) + nvl(new.refund_count, 0),
       nvl(old.refund_num, 0) + nvl(new.refund_num, 0),
       nvl(old.refund_amount, 0) + nvl(new.refund_amount, 0),
       nvl(new.cart_last_30d_count, 0),
       nvl(old.cart_count, 0) + nvl(new.cart_count, 0),
       nvl(new.favor_last_30d_count, 0),
       nvl(old.favor_count, 0) + nvl(new.favor_count, 0),
       nvl(new.appraise_last_30d_good_count, 0),
       nvl(new.appraise_last_30d_mid_count, 0),
       nvl(new.appraise_last_30d_bad_count, 0),
       nvl(new.appraise_last_30d_default_count, 0),
       nvl(old.appraise_good_count, 0) + nvl(new.appraise_good_count, 0),
       nvl(old.appraise_mid_count, 0) + nvl(new.appraise_mid_count, 0),
       nvl(old.appraise_bad_count, 0) + nvl(new.appraise_bad_count, 0),
       nvl(old.appraise_default_count, 0) + nvl(new.appraise_default_count, 0)
from $app.dwt_sku_topic old
         full join (
    select sku_id,
           sum(order_count)                                      order_last_30d_count,
           sum(order_num)                                        order_last_30d_num,
           sum(order_amount)                                     order_last_30d_amount,
           sum(if(dt = '$day', order_count, 0))            order_count,
           sum(if(dt = '$day', order_num, 0))              order_num,
           sum(if(dt = '$day', order_amount, 0.0))         order_amount,
           sum(payment_count)                                    payment_last_30d_count,
           sum(payment_num)                                      payment_last_30d_num,
           sum(payment_amount)                                   payment_last_30d_amount,
           sum(if(dt = '$day', payment_count, 0))          payment_count,
           sum(if(dt = '$day', payment_num, 0))            payment_num,
           sum(if(dt = '$day', payment_amount, 0.0))       payment_amount,
           sum(refund_count)                                     refund_last_30d_count,
           sum(refund_num)                                       refund_last_30d_num,
           sum(refund_amount)                                    refund_last_30d_amount,
           sum(if(dt = '$day', refund_count, 0))           refund_count,
           sum(if(dt = '$day', refund_num, 0))             refund_num,
           sum(if(dt = '$day', refund_amount, 0.0))        refund_amount,
           sum(cart_count)                                       cart_last_30d_count,
           sum(if(dt = '$day', cart_count, 0))             cart_count,
           sum(favor_count)                                      favor_last_30d_count,
           sum(if(dt = '$day', favor_count, 0))            favor_count,
           sum(appraise_good_count)                              appraise_last_30d_good_count,
           sum(appraise_mid_count)                               appraise_last_30d_mid_count,
           sum(appraise_bad_count)                               appraise_last_30d_bad_count,
           sum(appraise_default_count)                           appraise_last_30d_default_count,
           sum(if(dt = '$day', appraise_good_count, 0))    appraise_good_count,
           sum(if(dt = '$day', appraise_mid_count, 0))     appraise_mid_count,
           sum(if(dt = '$day', appraise_bad_count, 0))     appraise_bad_count,
           sum(if(dt = '$day', appraise_default_count, 0)) appraise_default_count
    from $app.dws_sku_action_daycount
    where dt between date_sub('$day', 30) and '$day'
    group by sku_id
) new
         left join (
    select id, spu_id
    from $app.dwd_dim_sku_info
    where dt = '$day'
) sku on nvl(new.sku_id, old.sku_id) = sku.id;
"
echo "$sql"
