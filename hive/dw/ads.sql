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

drop table if exists ads_new_mid_count;
create external table ads_new_mid_count
(
    `create_date`   string comment '创建时间',
    `new_mid_count` BIGINT comment '新增设备数量'
) COMMENT '每日新增设备数量'
    row format delimited fields terminated by '\t'
    location '/warehouse/gmall/ads/ads_new_mid_count/';

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

-- 只在安装当天启动过，且启动时间是在7天前
drop table if exists ads_silent_count;
create external table ads_silent_count
(
    `dt`           string COMMENT '统计日期',
    `silent_count` bigint COMMENT '沉默设备数'
) COMMENT '沉默用户数'
    row format delimited fields terminated by '\t'
    location '/warehouse/gmall/ads/ads_silent_count';

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

