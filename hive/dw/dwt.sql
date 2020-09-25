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
drop table if exists dwt_sku_topic;
create table dwt_sku_topic
(
    sku_id                          string comment 'sku_id',
    spu_id                          string comment 'spu_id',
    order_last_30d_count            bigint comment '最近30日被下单次数',
    order_last_30d_num              bigint comment '最近30日被下单件数',
    order_last_30d_amount           decimal(16, 2) comment '最近30日被下单金额',
    order_count                     bigint comment '累积被下单次数',
    order_num                       bigint comment '累积被下单件数',
    order_amount                    decimal(16, 2) comment '累积被下单金额',
    payment_last_30d_count          bigint comment '最近30日被支付次数',
    payment_last_30d_num            bigint comment '最近30日被支付件数',
    payment_last_30d_amount         decimal(16, 2) comment '最近30日被支付金额',
    payment_count                   bigint comment '累积被支付次数',
    payment_num                     bigint comment '累积被支付件数',
    payment_amount                  decimal(16, 2) comment '累积被支付金额',
    refund_last_30d_count           bigint comment '最近三十日退款次数',
    refund_last_30d_num             bigint comment '最近三十日退款件数',
    refund_last_30d_amount          decimal(16, 2) comment '最近三十日退款金额',
    refund_count                    bigint comment '累积退款次数',
    refund_num                      bigint comment '累积退款件数',
    refund_amount                   decimal(16, 2) comment '累积退款金额',
    cart_last_30d_count             bigint comment '最近30日被加入购物车次数',
    cart_count                      bigint comment '累积被加入购物车次数',
    favor_last_30d_count            bigint comment '最近30日被收藏次数',
    favor_count                     bigint comment '累积被收藏次数',
    appraise_last_30d_good_count    bigint comment '最近30日好评数',
    appraise_last_30d_mid_count     bigint comment '最近30日中评数',
    appraise_last_30d_bad_count     bigint comment '最近30日差评数',
    appraise_last_30d_default_count bigint comment '最近30日默认评价数',
    appraise_good_count             bigint comment '累积好评数',
    appraise_mid_count              bigint comment '累积中评数',
    appraise_bad_count              bigint comment '累积差评数',
    appraise_default_count          bigint comment '累积默认评价数'
) COMMENT '商品主题宽表'
    stored as parquet
    location '/warehouse/gmall/dwt/dwt_sku_topic/'
    tblproperties ("parquet.compression" = "lzo");
truncate table dwt_sku_topic;
