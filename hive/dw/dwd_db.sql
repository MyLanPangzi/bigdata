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

