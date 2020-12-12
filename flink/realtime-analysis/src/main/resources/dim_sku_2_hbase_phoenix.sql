CREATE TABLE base_trademark
(
    tm_id   bigint comment '品牌id',
    tm_name varchar(20) comment '品牌名称',
    primary key (tm_id) not enforced
)WITH (
    'connector' = 'mysql-cdc',
    'hostname' = 'hadoop102',
    'port' = '3306',
    'username' = 'root',
    'password' = '000000',
    'database-name' = 'gmall2020',
    'table-name' = 'base_trademark'
);
CREATE TABLE base_category1
(
    id   bigint comment '编号',
    name varchar(10) comment '分类名称',
    primary key (id) not enforced
)WITH (
    'connector' = 'mysql-cdc',
    'hostname' = 'hadoop102',
    'port' = '3306',
    'username' = 'root',
    'password' = '000000',
    'database-name' = 'gmall2020',
    'table-name' = 'base_category1'
);
CREATE TABLE base_category2
(
    id           bigint comment '编号',
    name         varchar(200) comment '二级分类名称',
    category1_id bigint comment '一级分类编号',
    primary key (id) not enforced
)WITH (
    'connector' = 'mysql-cdc',
    'hostname' = 'hadoop102',
    'port' = '3306',
    'username' = 'root',
    'password' = '000000',
    'database-name' = 'gmall2020',
    'table-name' = 'base_category2'
);
CREATE TABLE base_category3
(
    id           bigint comment '编号',
    name         varchar(200) comment '三级分类名称',
    category2_id bigint comment '二级分类编号',
    primary key (id) not enforced
)WITH (
    'connector' = 'mysql-cdc',
    'hostname' = 'hadoop102',
    'port' = '3306',
    'username' = 'root',
    'password' = '000000',
    'database-name' = 'gmall2020',
    'table-name' = 'base_category3'
);
CREATE TABLE spu_info
(
    id           bigint comment '商品id',
    spu_name     varchar(200) comment '商品名称',
    description  varchar(1000) comment '商品描述(后台简述）',
    category3_id bigint comment '三级分类id',
    tm_id        bigint comment '品牌id',
    primary key (id) not enforced
)WITH (
    'connector' = 'mysql-cdc',
    'hostname' = 'hadoop102',
    'port' = '3306',
    'username' = 'root',
    'password' = '000000',
    'database-name' = 'gmall2020',
    'table-name' = 'spu_info'
);
CREATE TABLE sku_info
(
    id              bigint comment 'skuid(itemID)',
    spu_id          bigint comment 'spuid',
    price           decimal comment '价格',
    sku_name        varchar(200) comment 'sku名称',
    sku_desc        varchar(2000) comment '商品规格描述',
    weight          decimal(10, 2) comment '重量',
    tm_id           bigint comment '品牌(冗余)',
    category3_id    bigint comment '三级分类id（冗余)',
    sku_default_img varchar(200) comment '默认显示图片(冗余)',
    create_time     TIMESTAMP(3) comment '创建时间',
    proctime AS PROCTIME(),
    primary key (id) not enforced
)WITH (
    'connector' = 'mysql-cdc',
    'hostname' = 'hadoop102',
    'port' = '3306',
    'username' = 'root',
    'password' = '000000',
    'database-name' = 'gmall2020',
    'table-name' = 'sku_info'
);

CREATE TABLE hbase_base_trademark
(
    rowkey bigint comment '品牌id',
    cf ROW (
        tm_name varchar (20)
        ),
    primary key (rowkey) not enforced
)WITH (
 'connector' = 'hbase-2.2',
 'table-name' = 'base_trademark',
 'zookeeper.quorum' = 'hadoop102:2181'
);

CREATE TABLE hbase_base_category1
(
    rowkey bigint comment '编号',
    cf ROW (
        name varchar (10)
        ),
    primary key (rowkey) not enforced
)WITH (
 'connector' = 'hbase-2.2',
 'table-name' = 'base_category1',
 'zookeeper.quorum' = 'hadoop102:2181'
);

CREATE TABLE hbase_base_category2
(
    rowkey bigint comment '编号',
    cf ROW (
        name varchar (200),
        category1_id bigint
        ),
    primary key (rowkey) not enforced
)WITH(
 'connector' = 'hbase-2.2',
 'table-name' = 'base_category2',
 'zookeeper.quorum' = 'hadoop102:2181'
);

CREATE TABLE hbase_base_category3
(
    rowkey bigint comment '编号',
    cf ROW (
        name varchar (200),
        category2_id bigint
        ),
    primary key (rowkey) not enforced
)WITH (
 'connector' = 'hbase-2.2',
 'table-name' = 'base_category3',
 'zookeeper.quorum' = 'hadoop102:2181'
);

CREATE TABLE hbase_spu_info
(
    rowkey bigint comment '商品id',
    cf ROW (
        spu_name varchar (100),
        description varchar (1000),
        category3_id bigint,
        tm_id bigint
        ),
    primary key (rowkey) not enforced
)WITH(
 'connector' = 'hbase-2.2',
 'table-name' = 'spu_info',
 'zookeeper.quorum' = 'hadoop102:2181'
);

CREATE TABLE hbase_sku_info
(
    rowkey bigint comment 'skuid(itemID)',
    cf ROW (
        price varchar (200),
        weight varchar (200),
        sku_name varchar (200),
        sku_desc varchar (2000),
        sku_default_img varchar (200),
        spu_id bigint,
        spu_name varchar (200),
        description varchar (1000),
        tm_id bigint,
        tm_name varchar (20),
        category3_id bigint,
        category3_name varchar (200) ,
        category2_id bigint,
        category2_name varchar (200) ,
        category1_id bigint,
        category1_name varchar (200) ,
        create_time TIMESTAMP (3)
        ),
    primary key (rowkey) not enforced
)WITH(
 'connector' = 'hbase-2.2',
 'table-name' = 'sku_info',
 'zookeeper.quorum' = 'hadoop102:2181'
);

INSERT INTO hbase_base_trademark
SELECT tm_id, ROW (tm_name)
FROM base_trademark;

INSERT INTO hbase_base_category1
SELECT id, ROW (name)
FROM base_category1;

INSERT INTO hbase_base_category2
SELECT id, (name, category1_id)
FROM base_category2;

INSERT INTO hbase_base_category3
SELECT id, (name, category2_id)
FROM base_category3;

INSERT INTO hbase_spu_info
SELECT id, (spu_name, description, category3_id, tm_id)
FROM spu_info;

INSERT INTO hbase_sku_info
SELECT sku.id,
       (CAST(price AS STRING),
        CAST(weight AS STRING),
        sku_name,
        sku_desc,
        sku_default_img,
        spu.rowkey,
        spu.cf.spu_name,
        spu.cf.description,
        tm.rowkey,
        tm.cf.tm_name,
        sku.category3_id,
        c3.cf.name,
        c3.cf.category2_id,
        c2.cf.name,
        c2.cf.category1_id,
        c1.cf.name,
        create_time)
FROM sku_info sku
         LEFT JOIN hbase_spu_info FOR SYSTEM_TIME AS OF sku.proctime AS spu ON sku.spu_id = spu.rowkey
    LEFT JOIN hbase_base_trademark FOR SYSTEM_TIME AS OF sku.proctime AS tm ON tm.rowkey = sku.tm_id
    LEFT JOIN hbase_base_category3 FOR SYSTEM_TIME AS OF sku.proctime AS c3 ON sku.category3_id = c3.rowkey
    LEFT JOIN hbase_base_category2 FOR SYSTEM_TIME AS OF sku.proctime AS c2 ON c3.cf.category2_id = c2.rowkey
    LEFT JOIN hbase_base_category1 FOR SYSTEM_TIME AS OF sku.proctime AS c1 ON c2.cf.category1_id = c1.rowkey;

-- DROP VIEW IF EXISTS "sku_info";
-- CREATE VIEW "sku_info"
-- (
--     "rowkey" UNSIGNED_LONG   primary key,
--     "cf"."price" varchar (200) ,
--     "cf"."weight" varchar (200) ,
--     "cf"."sku_name" varchar (200),
--     "cf"."sku_desc" varchar (2000),
--     "cf"."sku_default_img" varchar (200),
--     "cf"."spu_id" UNSIGNED_LONG ,
--     "cf"."spu_name" varchar (200),
--     "cf"."description" varchar (1000),
--     "cf"."tm_id" UNSIGNED_LONG ,
--     "cf"."tm_name" varchar (20),
--     "cf"."category3_id" UNSIGNED_LONG ,
--     "cf"."category3_name" varchar (200) ,
--     "cf"."category2_id" UNSIGNED_LONG ,
--     "cf"."category2_name" varchar (200) ,
--     "cf"."category1_id" UNSIGNED_LONG ,
--     "cf"."category1_name" varchar (200) ,
--     "cf"."create_time" UNSIGNED_LONG
-- );
-- SELECT * FROM "sku_info";

