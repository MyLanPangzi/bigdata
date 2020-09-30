#!/usr/bin/env bash

day=$1
if [ -z "$day" ]; then
  day=$(date -d '-1 day' +%F)
fi


app=gmall
sql="
insert overwrite table $app.dwd_dim_user_info_tmp
select id,
       name,
       birthday,
       gender,
       email,
       user_level,
       create_time,
       operate_time,
       '$day',
       '9999-99-99'
from $app.ods_user_info
where dt = '$day'
union
select history.id,
       history.name,
       history.birthday,
       history.gender,
       history.email,
       history.user_level,
       history.create_time,
       history.operate_time,
       history.start_time,
       if(new.id is not null and history.end_time = '9999-99-99' and '$day' > history.start_time,
          date_sub('$day', 1), history.end_time)
from $app.dwd_dim_user_info history
         left join (
             select id,
                   name,
                   birthday,
                   gender,
                   email,
                   user_level,
                   create_time,
                   operate_time
            from $app.ods_user_info
            where dt = '$day'
         ) new on history.id = new.id;
insert overwrite table $app.dwd_dim_user_info
select id,
       name,
       birthday,
       gender,
       email,
       user_level,
       create_time,
       operate_time,
       start_time,
       end_time
from $app.dwd_dim_user_info_tmp;
"
echo "$sql"
