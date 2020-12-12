CREATE TABLE base_province
(
    id        bigint null comment 'id',
    name      varchar(20) null comment '省名称',
    region_id varchar(20) null comment '大区id',
    area_code varchar(20) null comment '行政区位码',
    iso_code  varchar(20) null comment '国际编码'
)WITH (
    'connector' = 'mysql-cdc',
    'hostname' = 'hadoop102',
    'port' = '3306',
    'username' = 'root',
    'password' = '000000',
    'database-name' = 'gmall2020',
    'table-name' = 'base_province'
);

CREATE TABLE base_region
(
    id          varchar(20) null comment '大区id',
    region_name varchar(20) null comment '大区名称'
)WITH (
    'connector' = 'mysql-cdc',
    'hostname' = 'hadoop102',
    'port' = '3306',
    'username' = 'root',
    'password' = '000000',
    'database-name' = 'gmall2020',
    'table-name' = 'base_region'
);

CREATE TABLE hbase_dim_base_province
(
    rowkey bigint,
    cf ROW (
        name varchar (20),
        area_code varchar (20),
        iso_code varchar (20),
        region_id varchar (20),
        region_name varchar (20)
        ),
    primary key (rowkey) not enforced

) WITH (
 'connector' = 'hbase-2.2',
 'table-name' = 'dim_base_province',
 'zookeeper.quorum' = 'hadoop102:2181'
);

INSERT INTO hbase_dim_base_province
select p.id,
       (name,
        area_code,
        iso_code,
        region_id,
        region_name)
from base_province p
         left join base_region r on p.region_id = r.id;

-- DROP VIEW IF EXISTS "dim_base_province";
-- CREATE VIEW "dim_base_province"
-- (
--     "rowkey" unsigned_long primary key,
--     "cf"."name" varchar (20),
--     "cf"."area_code" varchar (20),
--     "cf"."iso_code" varchar (20),
--     "cf"."region_id" varchar (20),
--     "cf"."region_name" varchar (20)
-- );
-- SELECT * FROM "dim_base_province";
--
