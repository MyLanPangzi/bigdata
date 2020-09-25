drop table if exists dwt_uv_topic;
create table dwt_uv_topic
(
    mid_id          string comment '设备ID',
    brand           string comment '品牌',
    model           string comment '手机型号',
    login_first_day string comment '首次登录日期',
    login_last_day  string comment '尾次登录日期',
    login_day_count bigint comment '当日活跃次数',
    login_count     bigint comment '累计活跃天数'
) comment '设备主题宽表'
    stored as parquet
    location '/warehouse/gmall/dwt/dwt_uv_topic/'
    tblproperties ('parquet.compression' = 'lzo');
-- truncate table dwt_uv_topic;
drop table if exists dwt_user_topic;
create table dwt_user_topic
(
    user_id                 string comment '用户id',
    login_date_first        string comment '首次登录时间',
    login_date_last         string comment '末次登录时间',
    login_count             bigint comment '累积登录天数',
    login_last_30d_count    bigint comment '最近30日登录天数',
    order_date_first        string comment '首次下单时间',
    order_date_last         string comment '末次下单时间',
    order_count             bigint comment '累积下单次数',
    order_amount            decimal(16, 2) comment '累积下单金额',
    order_last_30d_count    bigint comment '最近30日下单次数',
    order_last_30d_amount   bigint comment '最近30日下单金额',
    payment_date_first      string comment '首次支付时间',
    payment_date_last       string comment '末次支付时间',
    payment_count           decimal(16, 2) comment '累积支付次数',
    payment_amount          decimal(16, 2) comment '累积支付金额',
    payment_last_30d_count  decimal(16, 2) comment '最近30日支付次数',
    payment_last_30d_amount decimal(16, 2) comment '最近30日支付金额'
) comment '设备主题宽表'
    stored as parquet
    location '/warehouse/gmall/dwt/dwt_user_topic/'
    tblproperties ('parquet.compression' = 'lzo');
-- truncate table dwt_user_topic;
