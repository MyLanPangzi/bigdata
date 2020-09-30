#!/usr/bin/env bash

day=$1
if [ -z "$day" ]; then
  day=$(date -d '-1 day' +%F)
fi

app=gmall
sql="
insert overwrite table $app.ads_appraise_bad_topN
select *
from $app.ads_appraise_bad_topN
union all
select '$day'                                                                           dt,
       sku_id,
       appraise_bad_count /
       appraise_default_count + appraise_bad_count + appraise_mid_count + appraise_good_count appraise_bad_ratio
from $app.dws_sku_action_daycount
where dt between date_sub('$day', 30) and '$day'
order by appraise_bad_ratio desc
limit 10;
"
echo "$sql"
