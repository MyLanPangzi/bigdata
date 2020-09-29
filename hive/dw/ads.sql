drop table if exists ads_uv_count;
create external table ads_uv_count
(
    `dt`          string COMMENT '统计日期',
    `day_count`   bigint COMMENT '当日用户数量',
    `wk_count`    bigint COMMENT '当周用户数量',
    `mn_count`    bigint COMMENT '当月用户数量',
    `is_weekend`  string COMMENT 'Y,N是否是周末,用于得到本周最终结果',
    `is_monthend` string COMMENT 'Y,N是否是月末,用于得到本月最终结果'
) COMMENT '活跃设备数'
    row format delimited fields terminated by '\t'
    location '/warehouse/gmall/ads/ads_uv_count/';
insert overwrite table ads_uv_count
select *
from ads_uv_count
union all
select '2020-06-14',
       t.day_count,
       t.wk_count,
       t.mn_count,
       if('2020-06-14' = date_sub(next_day('2020-06-14', 'Monday'), 1), 'Y', 'N') is_weekend,
       if('2020-06-14' = last_day('2020-06-14'), 'Y', 'N')                        is_monthend

from (
         select sum(if(date_format(dt, 'yyyy-MM-dd') = '2020-06-14', 1, 0))                      day_count,
                sum(if(
                        dt between date_sub(next_day('2020-06-14', 'Monday'), 7) and date_sub(next_day('2020-06-14', 'Monday'), 1),
                        1, 0))                                                                   wk_count,
                sum(if(date_format(dt, 'yyyy-MM') = date_format('2020-06-14', 'yyyy-MM'), 1, 0)) mn_count
         from dws_uv_detail_daycount
         where dt between date_format('2020-06-14', 'yyyy-MM-01') and last_day('2020-06-14')
     ) t;

drop table if exists ads_new_mid_count;
create external table ads_new_mid_count
(
    `create_date`   string comment '创建时间',
    `new_mid_count` BIGINT comment '新增设备数量'
) COMMENT '每日新增设备数量'
    row format delimited fields terminated by '\t'
    location '/warehouse/gmall/ads/ads_new_mid_count/';
insert overwrite table ads_new_mid_count
select *
from ads_new_mid_count
union all
select '2020-06-14',
       count(*)
from dwt_uv_topic
where login_last_day = login_first_day
  and login_first_day = '2020-06-14';

drop table if exists ads_user_retention_day_rate;
create external table ads_user_retention_day_rate
(
    `stat_date`       string comment '统计日期',
    `create_date`     string comment '设备新增日期',
    `retention_day`   int comment '截止当前日期留存天数',
    `retention_count` bigint comment '留存数量',
    `new_mid_count`   bigint comment '设备新增数量',
    `retention_ratio` decimal(16, 2) comment '留存率'
) COMMENT '留存率'
    row format delimited fields terminated by '\t'
    location '/warehouse/gmall/ads/ads_user_retention_day_rate/';
insert overwrite table ads_user_retention_day_rate
select *
from ads_user_retention_day_rate
union all
select '2020-06-16'                                                                                 stat_date,
       date_format(date_sub('2020-06-16', 1), 'yyyy-MM-dd')                                         create_date,
       1                                                                                            retention_day,
       sum(if(login_first_day = date_sub('2020-06-16', 1) and login_last_day = '2020-06-16', 1, 0)) retention_count,
       sum(if(login_first_day = '2020-06-16', 1, 0))                                                new_mid_count,
       sum(if(login_first_day = date_sub('2020-06-16', 1), 1, 0)) /
       sum(if(login_first_day = '2020-06-16', 1, 0)) * 100                                          retention_ratio
from dwt_uv_topic
union all
select '2020-06-16'                                                                                 stat_date,
       date_format(date_sub('2020-06-16', 2), 'yyyy-MM-dd')                                         create_date,
       2                                                                                            retention_day,
       sum(if(login_first_day = date_sub('2020-06-16', 2) and login_last_day = '2020-06-16', 1, 0)) retention_count,
       sum(if(login_first_day = '2020-06-16', 1, 0))                                                new_mid_count,
       sum(if(login_first_day = date_sub('2020-06-16', 2), 1, 0)) /
       sum(if(login_first_day = '2020-06-16', 1, 0)) * 100                                          retention_ratio
from dwt_uv_topic
union all
select '2020-06-16'                                                                                 stat_date,
       date_format(date_sub('2020-06-16', 3), 'yyyy-MM-dd')                                         create_date,
       3                                                                                            retention_day,
       sum(if(login_first_day = date_sub('2020-06-16', 3) and login_last_day = '2020-06-16', 1, 0)) retention_count,
       sum(if(login_first_day = '2020-06-16', 3, 0))                                                new_mid_count,
       sum(if(login_first_day = date_sub('2020-06-16', 3), 1, 0)) /
       sum(if(login_first_day = '2020-06-16', 1, 0)) * 100                                          retention_ratio
from dwt_uv_topic;

-- 只在安装当天启动过，且启动时间是在7天前
drop table if exists ads_silent_count;
create external table ads_silent_count
(
    `dt`           string COMMENT '统计日期',
    `silent_count` bigint COMMENT '沉默设备数'
) COMMENT '沉默用户数'
    row format delimited fields terminated by '\t'
    location '/warehouse/gmall/ads/ads_silent_count';
insert overwrite table ads_silent_count
select *
from ads_silent_count
union all
select '2020-06-14' dt,
       count(*)     silent_count
from dwt_uv_topic
where login_last_day = login_first_day
  and login_first_day < date_sub('2020-06-14', 7);

-- 本周回流用户数
-- 上周未活跃，本周活跃的设备，且不是本周新增设备
drop table if exists ads_back_count;
create external table ads_back_count
(
    `dt`            string COMMENT '统计日期',
    `wk_dt`         string COMMENT '统计日期所在周',
    `wastage_count` bigint COMMENT '回流设备数'
) COMMENT '本周回流用户数'
    row format delimited fields terminated by '\t'
    location '/warehouse/gmall/ads/ads_back_count';
insert overwrite table ads_back_count
select *
from ads_back_count
union all
select '2020-06-14'                                                                                         dt,
       concat(date_sub(next_day('2020-06-14', 'Monday'), 7), date_sub(next_day('2020-06-14', 'Monday'), 1)) wk_dt,
       count(*)                                                                                             wastage_count
from dwt_uv_topic
where login_first_day < date_sub('2020-06-14', 7)
  and login_last_day between date_sub(next_day('2020-06-14', 'Monday'), 7) and date_sub(next_day('2020-06-14', 'Monday'), 1)
  and mid_id not in (
    select mid_id
    from dws_uv_detail_daycount
    where dt between date_sub(next_day('2020-06-14', 'Monday'), 14) and date_sub(next_day('2020-06-14', 'Monday'), 8)
    group by mid_id
);

-- 最近连续三周活跃用户数
drop table if exists ads_continuity_wk_count;
create external table ads_continuity_wk_count
(
    `dt`               string COMMENT '统计日期,一般用结束周周日日期,如果每天计算一次,可用当天日期',
    `wk_dt`            string COMMENT '持续时间',
    `continuity_count` bigint COMMENT '活跃数'
) COMMENT '最近连续三周活跃用户数'
    row format delimited fields terminated by '\t'
    location '/warehouse/gmall/ads/ads_continuity_wk_count';
with one as (
    select mid_id
    from dws_uv_detail_daycount
    where dt between date_sub(next_day('2020-06-14', 'Monday'), 7) and date_sub(next_day('2020-06-14', 'Monday'), 1)
    group by mid_id
),
     two as (
         select mid_id
         from dws_uv_detail_daycount
         where dt between date_sub(next_day('2020-06-14', 'Monday'), 7 * 2) and date_sub(next_day('2020-06-14', 'Monday'), 7 + 1)
         group by mid_id
     ),
     three as (
         select mid_id
         from dws_uv_detail_daycount
         where dt between date_sub(next_day('2020-06-14', 'Monday'), 7 * 3) and date_sub(next_day('2020-06-14', 'Monday'), 7 * 2 + 1)
         group by mid_id
     )
insert
overwrite
table
ads_continuity_wk_count
select *
from ads_continuity_wk_count
union all
select '2020-06-14'                                                                                         dt,
       concat(date_sub(next_day('2020-06-14', 'Monday'), 7), date_sub(next_day('2020-06-14', 'Monday'), 1)) wk_dt,
       count(*)                                                                                             continuity_count
from one
         inner join two on one.mid_id = two.mid_id
         inner join three on one.mid_id = three.mid_id;

drop table if exists ads_continuity_uv_count;
create external table ads_continuity_uv_count
(
    `dt`               string COMMENT '统计日期',
    `wk_dt`            string COMMENT '最近7天日期',
    `continuity_count` bigint
) COMMENT '最近七天内连续三天活跃用户数'
    row format delimited fields terminated by '\t'
    location '/warehouse/gmall/ads/ads_continuity_uv_count';
insert overwrite table ads_continuity_uv_count
select *
from ads_continuity_uv_count
union all
select '2020-06-14'                                                                                         dt,
       concat(date_sub(next_day('2020-06-14', 'Monday'), 7), date_sub(next_day('2020-06-14', 'Monday'), 1)) wk_dt,
       count(*)                                                                                             continuity_count
from (
         select mid_id
         from (
                  select mid_id, dt, datediff(dt, lag(dt, 2, '1970-01-01') over (partition by mid_id order by dt)) diff
                  from dws_uv_detail_daycount
                  where dt between date_sub('2020-06-14', 7) and '2020-06-14'
              ) t
         where t.diff = 2
         group by mid_id
     ) t;
