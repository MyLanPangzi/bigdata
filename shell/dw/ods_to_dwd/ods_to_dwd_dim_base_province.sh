#!/usr/bin/env bash

app=gmall
sql="
insert overwrite table $app.dwd_dim_base_province
select p.id,
       name,
       region_id,
       area_code,
       iso_code,
       region_name
from $app.ods_base_province p
         left join $app.ods_base_region r on p.region_id = r.id;
"
echo "$sql"
