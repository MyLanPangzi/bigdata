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
SET hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table $app.dwd_display_log partition (dt = '$day')
select get_json_object(line, '$.common.ar'),
       get_json_object(line, '$.common.ba'),
       get_json_object(line, '$.common.ch'),
       get_json_object(line, '$.common.md'),
       get_json_object(line, '$.common.mid'),
       get_json_object(line, '$.common.os'),
       get_json_object(line, '$.common.uid'),
       get_json_object(line, '$.common.vc'),
       get_json_object(line, '$.page.during_time'),
       get_json_object(line, '$.page.item'),
       get_json_object(line, '$.page.item_type'),
       get_json_object(line, '$.page.last_page_id'),
       get_json_object(line, '$.page.page_id'),
       get_json_object(line, '$.page.sourceType'),
       get_json_object(display, '$.displayType'),
       get_json_object(display, '$.item'),
       get_json_object(display, '$.item_type'),
       get_json_object(display, '$.order'),
       get_json_object(line, '$.ts')
from $app.ods_log lateral view $app.explode_json_array(get_json_object(line, '$.displays')) tmp as display
where get_json_object(line, '$.displays') is not null
  and dt = '$day';
"
echo "$sql"
hive -e "$sql"
