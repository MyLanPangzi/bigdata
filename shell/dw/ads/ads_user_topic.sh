#!/usr/bin/env bash

day=$1
if [ -z "$day" ]; then
  day=$(date -d '-1 day' +%F)
fi

app=gmall
sql="
insert overwrite table $app.ads_user_topic
select *
from $app.ads_user_topic
union all
select '$day'                                             dt,
       sum(if(login_date_last = '$day', 1, 0))            day_users,
       sum(if(login_date_first = '$day', 1, 0))           day_new_users,
       sum(if(payment_date_first = '$day', 1, 0))         day_new_payment_users,
       sum(if(payment_count > 0, 1, 0))                         payment_users,
       count(*)                                                 users,
       sum(if(login_date_last = '$day', 1, 0)) / count(*) day_users2users,
       sum(if(payment_count > 0, 1, 0)) / count(*)              payment_users2users,
       sum(if(login_date_last = '$day', 1, 0)) /
       sum(if(login_date_first = '$day', 1, 0))           day_new_users2users
from $app.dwt_user_topic;
"
echo "$sql"
