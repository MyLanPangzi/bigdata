#!/usr/bin/env bash

day=$1
if [ -z "$day" ]; then
  day=$(date -d '-1 day' +%F)
fi

app=gmall
sql="
insert into table $app.dwt_user_topic
select nvl(old.user_id, new.user_id),
       nvl(old.login_date_first, '$day'),
       if(new.user_id is null, old.login_date_first, '$day'),
       nvl(old.login_count, 0) + if(new.login_count > 0, 1, 0),
       new.login_last_30d_count,
       if(new.order_count > 0 and old.order_date_first is null, '$day', old.order_date_first),
       if(new.order_count > 0, '$day', order_date_last),
       nvl(old.order_count, 0) + nvl(new.order_count, 0),
       nvl(old.order_amount, 0.0) + nvl(new.order_amount, 0.0),
       new.order_last_30d_count,
       new.order_last_30d_amount,
       if(new.payment_count > 0 and old.payment_date_first is null, '$day', old.payment_date_first),
       if(new.payment_count > 0, '$day', old.payment_date_last),
       nvl(old.payment_count, 0) + nvl(new.payment_count, 0),
       nvl(old.payment_amount, 0.0) + nvl(new.payment_amount, 0.0) payment_amount,
       new.payment_last_30d_count,
       new.payment_last_30d_amount
from $app.dwt_user_topic old
         full join (
    select user_id,
           sum(if(dt = '$day', login_count, 0))      login_count,
           sum(if(dt = '$day', order_count, 0))      order_count,
           sum(if(dt = '$day', order_amount, 0.0))   order_amount,
           sum(if(dt = '$day', payment_count, 0.0))  payment_count,
           sum(if(dt = '$day', payment_amount, 0.0)) payment_amount,
           sum(if(login_count > 0, 1, 0))                  login_last_30d_count,
           sum(order_count)                                order_last_30d_count,
           sum(order_amount)                               order_last_30d_amount,
           sum(payment_count)                              payment_last_30d_count,
           sum(payment_amount)                             payment_last_30d_amount
    from $app.dws_user_action_daycount
    where dt between date_sub('$day', 30) and '$day'
    group by user_id
) new;
"
echo "$sql"
