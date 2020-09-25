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
