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
