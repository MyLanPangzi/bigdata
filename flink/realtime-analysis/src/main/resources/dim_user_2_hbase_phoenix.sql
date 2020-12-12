CREATE TABLE user_info
(
    id           bigint comment '编号',
    login_name   varchar(200) comment '用户名称',
    nick_name    varchar(200) comment '用户昵称',
    passwd       varchar(200) comment '用户密码',
    name         varchar(200) comment '用户姓名',
    phone_num    varchar(200) comment '手机号',
    email        varchar(200) comment '邮箱',
    head_img     varchar(200) comment '头像',
    user_level   varchar(200) comment '用户级别',
    birthday     date comment '用户生日',
    gender       varchar(1) comment '性别 M男,F女',
    create_time  TIMESTAMP(3) comment '创建时间',
    operate_time TIMESTAMP(3) comment '修改时间',
    primary key (id) not enforced
)WITH (
    'connector' = 'mysql-cdc',
    'hostname' = 'hadoop102',
    'port' = '3306',
    'username' = 'root',
    'password' = '000000',
    'database-name' = 'gmall2020',
    'table-name' = 'user_info'
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
        create_time TIMESTAMP(3) ,
        operate_time TIMESTAMP(3)
        ),
    primary key (rowkey) not enforced

) WITH (
 'connector' = 'hbase-2.2',
 'table-name' = 'user_info',
 'zookeeper.quorum' = 'hadoop102:2181'
)

INSERT INTO hbase_user_info
select id,
       (login_name,
        nick_name,
        passwd,
        name,
        phone_num,
        email,
        head_img,
        user_level,
        CAST(birthday AS STRING),
        gender,
        create_time,
        operate_time) cf
from user_info;
--
-- drop view "user_info";
-- CREATE VIEW "user_info"
-- (
--     "rowkey"           unsigned_long  primary key,
--     "cf"."login_name"   varchar(200) ,
--     "cf"."nick_name"    varchar(200),
--     "cf"."passwd"       varchar(200),
--     "cf"."name"         varchar(200) ,
--     "cf"."phone_num"    varchar(200),
--     "cf"."email"        varchar(200),
--     "cf"."head_img"     varchar(200) ,
--     "cf"."user_level"   varchar(200) ,
--     "cf"."birthday"     varchar(200) ,
--     "cf"."gender"       varchar(1) ,
--     "cf"."create_time"  UNSIGNED_LONG   ,
--     "cf"."operate_time" UNSIGNED_LONG
-- );
--
-- select *
-- from "user_info";
