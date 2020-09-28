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