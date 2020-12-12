CREATE TABLE order_info
(
    id                    bigint comment '编号',
    consignee             varchar(100) comment '收货人',
    consignee_tel         varchar(20) comment '收件人电话',
    final_total_amount    decimal(16, 2) comment '总金额',
    order_status          varchar(20) comment '订单状态',
    user_id               bigint comment '用户id',
    delivery_address      varchar(1000) comment '送货地址',
    order_comment         varchar(200) comment '订单备注',
    out_trade_no          varchar(50) comment '订单交易编号（第三方支付用)',
    trade_body            varchar(200) comment '订单描述(第三方支付用)',
    create_time           TIMESTAMP comment '创建时间',
    operate_time          TIMESTAMP comment '操作时间',
    expire_time           TIMESTAMP comment '失效时间',
    tracking_no           varchar(100) comment '物流单编号',
    parent_order_id       bigint comment '父订单编号',
    img_url               varchar(200) comment '图片路径',
    province_id           int comment '地区',
    benefit_reduce_amount decimal(16, 2) comment '优惠金额',
    original_total_amount decimal(16, 2) comment '原价金额',
    feight_fee            decimal(16, 2) comment '运费',
    proctime AS PROCTIME(),
    primary key (id) not enforced
) WITH (
 'connector' = 'mysql-cdc',
 'hostname' = 'hadoop102',
 'port' = '3306',
 'username' = 'root',
 'password' = '000000',
 'database-name' = 'gmall2020',
 'table-name' = 'order_info'
);
CREATE TABLE hbase_base_province
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
CREATE TABLE hbase_user_info
(
    rowkey bigint,
    cf ROW (
        login_name varchar (200) ,
        nick_name varchar (200) ,
        passwd varchar (200) ,
        name varchar (200) ,
        phone_num varchar (200),
        email varchar (200) ,
        head_img varchar (200) ,
        user_level varchar (200) ,
        birthday STRING ,
        gender varchar (1) ,
        create_time TIMESTAMP (3) ,
        operate_time TIMESTAMP (3)
        ),
    primary key (rowkey) not enforced

) WITH (
 'connector' = 'hbase-2.2',
 'table-name' = 'user_info',
 'zookeeper.quorum' = 'hadoop102:2181'
);

CREATE TABLE es_first_order_index
(
    id                    BIGINT,
    order_status          STRING,
    user_id               BIGINT,
    final_total_amount    DOUBLE,
    benefit_reduce_amount DOUBLE,
    original_total_amount DOUBLE,
    feight_fee            DOUBLE,
    expire_time           STRING,
    create_time           STRING,
    operate_time          STRING,
    create_date           STRING,
    create_hour           STRING,
    if_first_order        BOOLEAN,
    province_id           BIGINT,
    province_name         STRING,
    province_area_code    STRING,
    province_iso_code     STRING,
    user_age_group        STRING,
    user_gender           STRING,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
  'connector' = 'elasticsearch-6',
  'hosts' = 'http://hadoop102:9200；http://hadoop103:9200；http://hadoop104:9200',
  'index' = 'first_order_{create_date}',
  'document-type' = '_doc'
);
CREATE TABLE hbase_user_first_order_status
(
    rowkey bigint,
    cf ROW (
        `status` BOOLEAN
        ),
    primary key (rowkey) not enforced

) WITH (
 'connector' = 'hbase-2.2',
 'table-name' = 'user_first_order_status',
 'zookeeper.quorum' = 'hadoop102:2181'
);

CREATE VIEW first_order_view
AS
SELECT o.id,
       order_status,
       user_id,
       CAST(final_total_amount AS DOUBLE)     final_total_amount,
       CAST(benefit_reduce_amount AS DOUBLE)  benefit_reduce_amount,
       CAST(original_total_amount AS DOUBLE)  original_total_amount,
       CAST(feight_fee AS DOUBLE)             feight_fee,
       CAST(o.expire_time AS STRING),
       CAST(o.create_time AS STRING),
       CAST(o.operate_time AS STRING),
       DATE_FORMAT(o.create_time, 'yyyyMMdd') create_date,
       DATE_FORMAT(o.create_time, 'HH')       create_hour,
       s.rowkey IS NULL                       if_first_order,
       p.rowkey                               province_id,
       p.cf.name                              province_name,
       p.cf.area_code                         province_area_code,
       p.cf.iso_code                          province_iso_code,
       u.cf.birthday                          user_age_group,
       u.cf.gender                            user_gender
FROM order_info o
         LEFT JOIN hbase_user_info u on o.user_id = u.rowkey
         LEFT JOIN hbase_base_province p on o.province_id = p.rowkey
         LEFT JOIN hbase_user_first_order_status FOR SYSTEM_TIME AS OF o.proctime AS s ON o.user_id = s.rowkey;

INSERT INTO es_first_order_index
SELECT *
FROM first_order_view;

INSERT INTO hbase_user_first_order_status
select user_id, ROW (if_first_order) cf
from first_order_view
where if_first_order;

-- drop view if exists "user_first_order_status";
-- create view "user_first_order_status"("rowkey" unsigned_long  primary key,"cf"."status" BOOLEAN);
-- select * from "user_first_order_status";

