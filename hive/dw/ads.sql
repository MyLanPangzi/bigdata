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

-- 最近七天内连续三天活跃用户数
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

drop table if exists ads_user_topic;
create external table ads_user_topic
(
    `dt`                    string COMMENT '统计日期',
    `day_users`             bigint COMMENT '活跃会员数',
    `day_new_users`         bigint COMMENT '新增会员数',
    `day_new_payment_users` bigint COMMENT '新增消费会员数',
    `payment_users`         bigint COMMENT '总付费会员数',
    `users`                 bigint COMMENT '总会员数',
    `day_users2users`       decimal(16, 2) COMMENT '会员活跃率',
    `payment_users2users`   decimal(16, 2) COMMENT '会员付费率',
    `day_new_users2users`   decimal(16, 2) COMMENT '会员新鲜度'
) COMMENT '会员信息表'
    row format delimited fields terminated by '\t'
    location '/warehouse/gmall/ads/ads_user_topic';
insert overwrite table ads_user_topic
select *
from ads_user_topic
union all
select '2020-06-14'                                             dt,
       sum(if(login_date_last = '2020-06-14', 1, 0))            day_users,
       sum(if(login_date_first = '2020-06-14', 1, 0))           day_new_users,
       sum(if(payment_date_first = '2020-06-14', 1, 0))         day_new_payment_users,
       sum(if(payment_count > 0, 1, 0))                         payment_users,
       count(*)                                                 users,
       sum(if(login_date_last = '2020-06-14', 1, 0)) / count(*) day_users2users,
       sum(if(payment_count > 0, 1, 0)) / count(*)              payment_users2users,
       sum(if(login_date_last = '2020-06-14', 1, 0)) /
       sum(if(login_date_first = '2020-06-14', 1, 0))           day_new_users2users
from dwt_user_topic;
-- 7.3.2 漏斗分析
-- 统计“浏览首页->浏览商品详情页->加入购物车->下单->支付”的转化率
-- 思路：统计各个行为的人数，然后计算比值。
drop table if exists ads_user_action_convert_day;
create external table ads_user_action_convert_day
(
    `dt`                             string COMMENT '统计日期',
    `home_count`                     bigint COMMENT '浏览首页人数',
    `good_detail_count`              bigint COMMENT '浏览商品详情页人数',
    `home2good_detail_convert_ratio` decimal(16, 2) COMMENT '首页到商品详情转化率',
    `cart_count`                     bigint COMMENT '加入购物车的人数',
    `good_detail2cart_convert_ratio` decimal(16, 2) COMMENT '商品详情页到加入购物车转化率',
    `order_count`                    bigint COMMENT '下单人数',
    `cart2order_convert_ratio`       decimal(16, 2) COMMENT '加入购物车到下单转化率',
    `payment_count`                  bigint COMMENT '支付人数',
    `order2payment_convert_ratio`    decimal(16, 2) COMMENT '下单到支付的转化率'
) COMMENT '漏斗分析'
    row format delimited fields terminated by '\t'
    location '/warehouse/gmall/ads/ads_user_action_convert_day/';
with page as (
    select '2020-06-14'                           dt,
           sum(if(page_id = 'home', 1, 0))        home_count,
           sum(if(page_id = 'good_detail', 1, 0)) good_detail_count
    from dwd_page_log
    where dt = '2020-06-14'
),
     u as (
         select '2020-06-14'                     dt,
                sum(if(cart_count > 0, 1, 0))    cart_count,
                sum(if(order_count > 0, 1, 0))   order_count,
                sum(if(payment_count > 0, 1, 0)) payment_count
         from dws_user_action_daycount
         where dt = '2020-06-14'
     )
insert
overwrite
table
ads_user_action_convert_day
select *
from ads_user_action_convert_day
union all
select u.dt,
       home_count,
       good_detail_count,
       home_count / good_detail_count home2good_detail_convert_ratio,
       cart_count,
       good_detail_count / cart_count good_detail2cart_convert_ratio,
       order_count,
       cart_count / order_count       cart2order_convert_ratio,
       payment_count,
       payment_count / order_count    order2payment_convert_ratio

from page
         inner join u on page.dt = u.dt;

-- 商品个数信息
drop table if exists ads_product_info;
create external table ads_product_info
(
    `dt`      string COMMENT '统计日期',
    `sku_num` bigint COMMENT 'sku个数',
    `spu_num` bigint COMMENT 'spu个数'
) COMMENT '商品个数信息'
    row format delimited fields terminated by '\t'
    location '/warehouse/gmall/ads/ads_product_info';
with sku as (
    select '2020-06-14' dt,
           count(*)     sku_num
    from dwt_sku_topic
),
     spu as (
         select '2020-06-14' dt,
                count(*)     spu_num
         from (
                  select spu_id
                  from dwt_sku_topic
                  group by spu_id
              ) t
     )
insert
overwrite
table
ads_product_info
select *
from ads_product_info
union all
select '2020-06-14' dt,
       sku_num,
       spu_num
from sku
         inner join spu on sku.dt = spu.dt;

-- 商品销量排名
drop table if exists ads_product_sale_topN;
create external table ads_product_sale_topN
(
    `dt`             string COMMENT '统计日期',
    `sku_id`         string COMMENT '商品ID',
    `payment_amount` bigint COMMENT '销量'
) COMMENT '商品销量排名'
    row format delimited fields terminated by '\t'
    location '/warehouse/gmall/ads/ads_product_sale_topN';
insert overwrite table ads_product_sale_topN
select *
from ads_product_sale_topN
union all
select '2020-06-14'   dt,
       sku_id,
       payment_amount payment_amount
from dws_sku_action_daycount
where dt = '2020-06-14'
order by payment_amount desc
limit 10;

-- 商品收藏排名
drop table if exists ads_product_favor_topN;
create external table ads_product_favor_topN
(
    `dt`          string COMMENT '统计日期',
    `sku_id`      string COMMENT '商品ID',
    `favor_count` bigint COMMENT '收藏量'
) COMMENT '商品收藏排名'
    row format delimited fields terminated by '\t'
    location '/warehouse/gmall/ads/ads_product_favor_topN';
insert overwrite table ads_product_favor_topN
select *
from ads_product_favor_topN
union all
select '2020-06-14' dt,
       sku_id,
       favor_count  favor_count
from dws_sku_action_daycount
where dt = '2020-06-14'
order by favor_count desc
limit 10;

-- 商品加入购物车排名
drop table if exists ads_product_cart_topN;
create external table ads_product_cart_topN
(
    `dt`         string COMMENT '统计日期',
    `sku_id`     string COMMENT '商品ID',
    `cart_count` bigint COMMENT '加入购物车次数'
) COMMENT '商品加入购物车排名'
    row format delimited fields terminated by '\t'
    location '/warehouse/gmall/ads/ads_product_cart_topN';
insert overwrite table ads_product_cart_topN
select *
from ads_product_cart_topN
union all
select '2020-06-14' dt,
       sku_id,
       cart_count   cart_count
from dws_sku_action_daycount
where dt = '2020-06-14'
order by cart_count desc
limit 10;

--  商品退款率排名（最近30天）
drop table if exists ads_product_refund_topN;
create external table ads_product_refund_topN
(
    `dt`           string COMMENT '统计日期',
    `sku_id`       string COMMENT '商品ID',
    `refund_ratio` decimal(16, 2) COMMENT '退款率'
) COMMENT '商品退款率排名'
    row format delimited fields terminated by '\t'
    location '/warehouse/gmall/ads/ads_product_refund_topN';
insert overwrite table ads_product_refund_topN
select *
from ads_product_refund_topN
union all
select '2020-06-14'                 dt,
       sku_id,
       refund_count / payment_count refund_ratio
from dws_sku_action_daycount
where dt between date_sub('2020-06-14', 30) and '2020-06-14'
order by refund_ratio desc
limit 10;

-- 商品差评率
drop table if exists ads_appraise_bad_topN;
create external table ads_appraise_bad_topN
(
    `dt`                 string COMMENT '统计日期',
    `sku_id`             string COMMENT '商品ID',
    `appraise_bad_ratio` decimal(16, 2) COMMENT '差评率'
) COMMENT '商品差评率'
    row format delimited fields terminated by '\t'
    location '/warehouse/gmall/ads/ads_appraise_bad_topN';
insert overwrite table ads_appraise_bad_topN
select *
from ads_appraise_bad_topN
union all
select '2020-06-14'                                                                           dt,
       sku_id,
       appraise_bad_count /
       appraise_default_count + appraise_bad_count + appraise_mid_count + appraise_good_count appraise_bad_ratio
from dws_sku_action_daycount
where dt between date_sub('2020-06-14', 30) and '2020-06-14'
order by appraise_bad_ratio desc
limit 10;

-- 下单数目统计
drop table if exists ads_order_daycount;
create external table ads_order_daycount
(
    dt           string comment '统计日期',
    order_count  bigint comment '单日下单笔数',
    order_amount bigint comment '单日下单金额',
    order_users  bigint comment '单日下单用户数'
) comment '下单数目统计'
    row format delimited fields terminated by '\t'
    location '/warehouse/gmall/ads/ads_order_daycount';
insert overwrite table ads_order_daycount
select *
from ads_order_daycount
union all
select '2020-06-14'                   dt,
       sum(order_count)               order_count,
       sum(order_amount)              order_amount,
       sum(if(order_count > 0, 1, 0)) order_users
from dwt_user_topic;

-- 支付信息统计
-- 每日支付金额、支付人数、支付商品数、支付笔数以及下单到支付的平均时长（取自DWD）
drop table if exists ads_payment_daycount;
create external table ads_payment_daycount
(
    dt                 string comment '统计日期',
    order_count        bigint comment '单日支付笔数',
    order_amount       bigint comment '单日支付金额',
    payment_user_count bigint comment '单日支付人数',
    payment_sku_count  bigint comment '单日支付商品数',
    payment_avg_time   decimal(16, 2) comment '下单到支付的平均时长，取分钟数'
) comment '支付信息统计'
    row format delimited fields terminated by '\t'
    location '/warehouse/gmall/ads/ads_payment_daycount';
with u as (
    select '2020-06-14'                     dt,
           sum(payment_count)               order_count,
           sum(payment_amount)              order_amount,
           sum(if(payment_count > 0, 1, 0)) payment_user_count
    from dwt_user_topic
),
     sku as (
         select '2020-06-14' dt,
                count(*)     payment_sku_count
         from dws_sku_action_daycount
         where payment_count > 0
           and dt = '2020-06-14'
     ),
     pay_avg as (
         select '2020-06-14'                                                         dt,
                avg(unix_timestamp(create_time) - unix_timestamp(payment_time)) / 60 payment_avg_time
         from dwd_fact_order_info
         where payment_time is not null
           and dt = '2020-06-14'
     )
insert
overwrite
table
ads_payment_daycount
select *
from ads_payment_daycount
union all
select u.dt,
       order_count,
       order_amount,
       payment_user_count,
       payment_sku_count,
       payment_avg_time
from u
         inner join sku on u.dt = sku.dt
         inner join pay_avg on pay_avg.dt = u.dt;

--  地区主题信息
drop table if exists ads_area_topic;
create external table ads_area_topic
(
    `dt`                 string COMMENT '统计日期',
    `id`                 bigint COMMENT '编号',
    `province_name`      string COMMENT '省份名称',
    `area_code`          string COMMENT '地区编码',
    `iso_code`           string COMMENT 'iso编码',
    `region_id`          string COMMENT '地区ID',
    `region_name`        string COMMENT '地区名称',
    `login_day_count`    bigint COMMENT '当天活跃设备数',
    `order_day_count`    bigint COMMENT '当天下单次数',
    `order_day_amount`   decimal(20, 2) COMMENT '当天下单金额',
    `payment_day_count`  bigint COMMENT '当天支付次数',
    `payment_day_amount` decimal(20, 2) COMMENT '当天支付金额'
) COMMENT '地区主题信息'
    row format delimited fields terminated by '\t'
    location '/warehouse/gmall/ads/ads_area_topic/';
insert overwrite table ads_area_topic
select *
from ads_area_topic
union all
select '2020-06-14' dt,
       id,
       province_name,
       area_code,
       iso_code,
       region_id,
       region_name,
       cast(login_day_count as bigint),
       order_day_count,
       order_day_amount,
       payment_day_count,
       payment_day_amount
from dwt_area_topic;

drop table ads_sale_tm_category1_stat_mn;
create external table ads_sale_tm_category1_stat_mn
(
    tm_id                 string comment '品牌id',
    category1_id          string comment '1级品类id ',
    category1_name        string comment '1级品类名称 ',
    buycount              bigint comment '购买人数',
    buy_twice_last        bigint comment '两次以上购买人数',
    buy_twice_last_ratio  decimal(16, 2) comment '单次复购率',
    buy_3times_last       bigint comment '三次以上购买人数',
    buy_3times_last_ratio decimal(16, 2) comment '多次复购率',
    stat_mn               string comment '统计月份',
    stat_date             string comment '统计日期'
) COMMENT '品牌复购率统计'
    row format delimited fields terminated by '\t'
    location '/warehouse/gmall/ads/ads_sale_tm_category1_stat_mn/';
with pay as (
    select sku.sku_id,
           sum(if(sku.order_count > 0, 1, 0)) buycount,
           sum(if(sku.order_count > 1, 1, 0)) buy_twice_last,
           sum(if(sku.order_count > 2, 1, 0)) buy_3times_last
    from dws_user_action_daycount lateral view explode(order_detail_stats) tmp as sku
    where date_format(dt, 'yyyy-MM') = date_format('2020-06-14', 'yyyy-MM')
      and order_detail_stats is not null
    group by sku.sku_id
),
     tm as (
         select tm_id,
                category1_id,
                category1_name,
                buycount,
                buy_twice_last,
                buy_3times_last
         from pay
                  inner join (
             select id,
                    tm_id,
                    category1_id,
                    category1_name
             from dwd_dim_sku_info
             where dt = '2020-06-14'
         ) sku on pay.sku_id = sku.id
     )
insert
overwrite
table
ads_sale_tm_category1_stat_mn
select *
from ads_sale_tm_category1_stat_mn
union all
select tm_id,
       category1_id,
       category1_name,
       sum(buycount)                        buycount,
       sum(buy_twice_last)                  buy_twice_last,
       sum(buy_3times_last)                 buy_3times_last,
       sum(buy_twice_last) / sum(buycount)  buy_twice_last_ratio,
       sum(buy_3times_last) / sum(buycount) buy_3times_last_ratio,
       date_format('2020-06-14', 'yyyy-MM') stat_mn,
       '2020-06-14'                         stat_date
from tm
group by tm_id, category1_id, category1_name;

