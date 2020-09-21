#!/usr/bin/env bash

day=$1
if [ -z "$day" ]; then
  day=$(date -d '-1 day' +%F)
fi
echo "$day"
hive=/opt/module/hive/bin/hive
echo "$hive"
app=gmall
sql="
insert overwrite table $app.dwd_start_log partition (dt = '$day')
select get_json_object(line,'$.common.ar'),
       get_json_object(line,'$.common.ba'),
       get_json_object(line,'$.common.ch'),
       get_json_object(line,'$.common.md'),
       get_json_object(line,'$.common.mid'),
       get_json_object(line,'$.common.os'),
       get_json_object(line,'$.common.uid'),
       get_json_object(line,'$.common.vc'),
       get_json_object(line,'$.start.entry'),
       get_json_object(line,'$.start.loading_time'),
       get_json_object(line,'$.start.open_ad_id'),
       get_json_object(line,'$.start.open_ad_ms'),
       get_json_object(line,'$.start.open_ad_skip_ms'),
       get_json_object(line,'$.ts')
from $app.ods_log
where get_json_object(line, '$.start') is not null
  and dt = '$day';
"
echo "$sql"
hive -e "$sql"
