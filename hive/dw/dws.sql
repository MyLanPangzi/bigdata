drop table if exists dws_uv_detail_daycount;
create external table dws_uv_detail_daycount
(
    mid_id      string comment '设备ID',
    brand       string comment '品牌',
    model       string comment '手机型号',
    login_count bigint comment '活跃次数',
    page_stats  array<struct<page_id:string,page_count:bigint>> comment 'pv访问统计'
) comment '每日设备行为表'
    partitioned by (dt string)
    stored as parquet
    location '/warehouse/gmall/dws/dws_uv_detail_daycount/'
    tblproperties ('parquet.compression' = 'lzo');
drop table if exists dws_user_action_daycount;
create table dws_user_action_daycount
(
    user_id            string comment '设备ID',
    login_count        bigint comment '登录次数',
    cart_count         bigint comment '加入购物车次数',
    order_count        bigint comment '下单次数',
    order_amount       decimal(10, 2) comment '下单金额',
    payment_count      bigint comment '支付次数',
    payment_amount     decimal(10, 2) comment '支付金额',
    order_detail_stats array<struct<sku_id :string,sku_num :bigint,order_count :bigint,order_amount
                                    :decimal(10, 2)>> comment '订单明细统计'
) comment '每日会员行为表'
    partitioned by (dt string)
    stored as parquet
    location '/warehouse/gmall/dws/dws_user_action_daycount/'
    tblproperties ('parquet.compression' = 'lzo');
-- truncate table dws_user_action_daycount;
drop table if exists dws_sku_action_daycount;
create table dws_sku_action_daycount
(
    sku_id                 string comment 'sku_id',
    order_count            bigint comment '被下单次数',
    order_num              bigint comment '被下单件数',
    order_amount           decimal(16, 2) comment '被下单金额',
    payment_count          bigint comment '被支付次数',
    payment_num            bigint comment '被支付件数',
    payment_amount         decimal(16, 2) comment '被支付金额',
    refund_count           bigint comment '被退款次数',
    refund_num             bigint comment '被退款件数',
    refund_amount          decimal(16, 2) comment '被退款金额',
    cart_count             bigint comment '被加入购物车次数',
    favor_count            bigint comment '被收藏次数',
    appraise_good_count    bigint comment '好评数',
    appraise_mid_count     bigint comment '中评数',
    appraise_bad_count     bigint comment '差评数',
    appraise_default_count bigint comment '默认评价数'
) COMMENT '每日商品行为'
    PARTITIONED BY (`dt` string)
    stored as parquet
    location '/warehouse/gmall/dws/dws_sku_action_daycount/'
    tblproperties ("parquet.compression" = "lzo");
truncate table dws_sku_action_daycount;
