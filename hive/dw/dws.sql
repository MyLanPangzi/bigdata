drop table if exists dws_uv_detail_daycount;
create external table dws_uv_detail_daycount
(
    mid_id      string comment '设备ID',
    brand       string comment '品牌',
    model       string comment '手机型号',
    login_count bigint comment '活跃次数',
    page_stats  struct<page_id:string,page_count:bigint> comment 'pv访问统计'
) comment '每日设备行为表'
    partitioned by (dt string)
    stored as parquet
    location '/warehouse/gmall/dws/dws_uv_detail_daycount/'
    tblproperties ('parquet.compression' = 'lzo');
with login as (
    select mid_id,
           brand,
           model,
           count(*) login_count
    from dwd_start_log
    where dt = '2020-06-14'
    group by mid_id, brand, model
),
     page as (
         select mid_id,
                brand,
                model,
                collect_set(named_struct('page_id', page_id, 'page_count', page_count)) page_stats
         from (
                  select mid_id,
                         brand,
                         model,
                         page_id,
                         count(*) page_count
                  from dwd_page_log
                  where dt = '2020-06-14'
                  group by mid_id, brand, model, page_id
              ) t
     )
insert
overwrite
table
dws_uv_detail_daycount
partition
(
dt = '2020-06-14'
)
select nvl(l.mid_id, p.mid_id),
       nvl(l.brand, p.brand),
       nvl(l.model, p.model),
       login_count,
       page_stats
from login l
         full join page p on l.mid_id = p.mid_id;

