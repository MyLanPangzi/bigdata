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
