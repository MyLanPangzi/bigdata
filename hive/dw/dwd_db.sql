use gmall;
drop table if exists dwd_dim_sku_info;
create table dwd_dim_sku_info
(
    id             string,
    spu_id         string,
    spu_name       string,
    price          decimal(16, 2),
    sku_name       string,
    sku_desc       string,
    weight         string,
    tm_id          string,
    tm_name        string,
    category3_id   string,
    category2_id   string,
    category1_id   string,
    category3_name string,
    category2_name string,
    category1_name string,
    create_time    string
)
    partitioned by (dt string)
    stored as parquet
    location '/warehouse/gmall/dwd/dwd_dim_sku_info'
    tblproperties ('parquet.compression' = 'lzo');
drop table if exists dwd_dim_coupon_info;
create table dwd_dim_coupon_info
(
    id               string,
    coupon_name      string,
    coupon_type      string,
    condition_amount decimal(16, 2),
    condition_num    bigint,
    activity_id      string,
    benefit_amount   decimal(16, 2),
    benefit_discount decimal(16, 2),
    create_time      string,
    range_type       string,
    spu_id           string,
    tm_id            string,
    category3_id     string,
    limit_num        bigint,
    operate_time     string,
    expire_time      string
)
    partitioned by (dt string)
    stored as parquet
    location '/warehouse/gmall/dwd/dwd_dim_coupon_info'
    tblproperties ('parquet.compression' = 'lzo');
drop table if exists dwd_dim_activity_info;
create table dwd_dim_activity_info
(

    id            string,
    activity_name string,
    activity_type string,
    start_time    string,
    end_time      string,
    create_time   string
)
    partitioned by (dt string)
    stored as parquet
    location '/warehouse/gmall/dwd/dwd_dim_activity_info'
    tblproperties ('parquet.compression' = 'lzo');
drop table if exists dwd_dim_base_province;
create table dwd_dim_base_province
(
    id          bigint,
    name        string,
    region_id   string,
    area_code   string,
    iso_code    string,
    region_name string
)
    stored as parquet
    location '/warehouse/gmall/dwd/dwd_dim_base_province'
    tblproperties ('parquet.compression' = 'lzo');
drop table if exists `dwd_dim_date_info`;
create external table `dwd_dim_date_info`
(
    `date_id`    string comment '日',
    `week_id`    string comment '周',
    `week_day`   string comment '周的第几天',
    `day`        string comment '每月的第几天',
    `month`      string comment '第几月',
    `quarter`    string comment '第几季度',
    `year`       string comment '年',
    `is_workday` string comment '是否是周末',
    `holiday_id` string comment '是否是节假日'
) comment '时间维度表'
    stored as parquet
    location '/warehouse/gmall/dwd/dwd_dim_date_info/'
    tblproperties ("parquet.compression" = "lzo");
create external table `dwd_dim_date_info_tmp`
(
    `date_id`    string comment '日',
    `week_id`    string comment '周',
    `week_day`   string comment '周的第几天',
    `day`        string comment '每月的第几天',
    `month`      string comment '第几月',
    `quarter`    string comment '第几季度',
    `year`       string comment '年',
    `is_workday` string comment '是否是周末',
    `holiday_id` string comment '是否是节假日'
) comment '时间维度表'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    location '/warehouse/gmall/dwd/dwd_dim_date_info_tmp/'
    tblproperties ("parquet.compression" = "lzo");
-- load data local inpath '/opt/module/db_log/date_info.txt' overwrite into table dwd_dim_date_info_tmp;
/*
insert overwrite table dwd_dim_date_info
select date_id,
       week_id,
       week_day,
       day,
       month,
       quarter,
       year,
       is_workday,
       holiday_id
from dwd_dim_date_info_tmp;
*/
drop table if exists dwd_fact_payment_info;
create table dwd_fact_payment_info
(
    id              string,
    out_trade_no    string,
    order_id        string,
    user_id         string,
    alipay_trade_no string,
    total_amount    decimal(16, 2),
    subject         string,
    payment_type    string,
    payment_time    string,
    province_id     string
)
    partitioned by (dt string)
    stored as parquet
    location '/warehouse/gmall/dwd/dwd_fact_payment_info'
    tblproperties ('parquet.compression' = 'lzo');
drop table if exists dwd_fact_refund_info;
create table dwd_fact_refund_info
(
    id                 string,
    user_id            string,
    order_id           string,
    sku_id             string,
    refund_type        string,
    refund_num         bigint,
    refund_amount      decimal(16, 2),
    refund_reason_type string,
    create_time        string
)
    partitioned by (dt string)
    stored as parquet
    location '/warehouse/gmall/dwd/dwd_fact_refund_info'
    tblproperties ('parquet.compression' = 'lzo');
drop table if exists dwd_fact_comment_info;
create table dwd_fact_comment_info
(
    id          string,
    user_id     string,
    sku_id      string,
    spu_id      string,
    order_id    string,
    appraise    string,
    create_time string
)
    partitioned by (dt string)
    stored as parquet
    location '/warehouse/gmall/dwd/dwd_fact_comment_info'
    tblproperties ('parquet.compression' = 'lzo');
drop table if exists dwd_fact_cart_info;
create table dwd_fact_cart_info
(

    id           string,
    user_id      string,
    sku_id       string,
    cart_price   decimal(16, 2),
    sku_num      bigint,
    sku_name     string,
    create_time  string,
    operate_time string,
    is_ordered   string,
    order_time   string,
    source_type  string,
    source_id    string
)
    partitioned by (dt string)
    stored as parquet
    location '/warehouse/gmall/dwd/dwd_fact_cart_info'
    tblproperties ('parquet.compression' = 'lzo');
drop table if exists dwd_fact_order_detail;
create table dwd_fact_order_detail
(

    id                        string,
    order_id                  string,
    user_id                   string,
    sku_id                    string,
    sku_name                  string,
    order_price               decimal(16, 2),
    sku_num                   bigint,
    create_time               string,
    source_type               string,
    source_id                 string,
    province_id               string,
    `original_amount_d`       decimal(20, 2) comment '原始价格分摊',
    `final_amount_d`          decimal(20, 2) comment '购买价格分摊',
    `feight_fee_d`            decimal(20, 2) comment '分摊运费',
    `benefit_reduce_amount_d` decimal(20, 2) comment '分摊优惠'
)
    partitioned by (dt string)
    stored as parquet
    location '/warehouse/gmall/dwd/dwd_fact_order_detail'
    tblproperties ('parquet.compression' = 'lzo');
drop table if exists dwd_fact_coupon_use;
create table dwd_fact_coupon_use
(

    id            string,
    coupon_id     string,
    user_id       string,
    order_id      string,
    coupon_status string,
    get_time      string,
    using_time    string,
    used_time     string
)
    partitioned by (dt string)
    stored as parquet
    location '/warehouse/gmall/dwd/dwd_fact_coupon_use'
    tblproperties ('parquet.compression' = 'lzo');
drop table if exists dwd_fact_order_info;
create table dwd_fact_order_info
(
    id                    string,
    final_total_amount    decimal(16, 2),
    order_status          string,
    user_id               string,
    out_trade_no          string,
    create_time           string,
    payment_time          string COMMENT '支付时间(已支付状态)',
    cancel_time           string COMMENT '取消时间(已取消状态)',
    finish_time           string COMMENT '完成时间(已完成状态)',
    refund_time           string COMMENT '退款时间(退款中状态)',
    refund_finish_time    string COMMENT '退款完成时间(退款完成状态)',
    activity_id           string,
    province_id           string,
    benefit_reduce_amount decimal(16, 2),
    original_total_amount decimal(16, 2),
    feight_fee            decimal(16, 2)
)
    partitioned by (dt string)
    stored as parquet
    location '/warehouse/gmall/dwd/dwd_fact_order_info'
    tblproperties ('parquet.compression' = 'lzo');
