CREATE TABLE order_info
(
	id bigint  comment '编号',
	consignee varchar(100)  comment '收货人',
	consignee_tel varchar(20)  comment '收件人电话',
	final_total_amount decimal(16,2)  comment '总金额',
	order_status varchar(20)  comment '订单状态',
	user_id bigint  comment '用户id',
	delivery_address varchar(1000)  comment '送货地址',
	order_comment varchar(200)  comment '订单备注',
	out_trade_no varchar(50)  comment '订单交易编号（第三方支付用)',
	trade_body varchar(200)  comment '订单描述(第三方支付用)',
	create_time TIMESTAMP  comment '创建时间',
	operate_time TIMESTAMP  comment '操作时间',
	expire_time TIMESTAMP  comment '失效时间',
	tracking_no varchar(100)  comment '物流单编号',
	parent_order_id bigint  comment '父订单编号',
	img_url varchar(200)  comment '图片路径',
	province_id int  comment '地区',
	benefit_reduce_amount decimal(16,2)  comment '优惠金额',
	original_total_amount decimal(16,2)  comment '原价金额',
	feight_fee decimal(16,2)  comment '运费',
	 primary key(id) not enforced
) WITH (
 'connector' = 'mysql-cdc',
 'hostname' = 'hadoop102',
 'port' = '3306',
 'username' = 'root',
 'password' = '000000',
 'database-name' = 'gmall2020',
 'table-name' = 'order_info'
);
CREATE TABLE order_detail
(
	id bigint comment '编号',
	order_id bigint  comment '订单编号',
	sku_id bigint  comment 'sku_id',
	sku_name varchar(200)  comment 'sku名称（冗余)',
	img_url varchar(200)  comment '图片名称（冗余)',
	order_price decimal(10,2)  comment '购买价格(下单时sku价格）',
	sku_num varchar(200)  comment '购买个数',
	create_time TIMESTAMP  comment '创建时间',
	source_type varchar(20)  comment '来源类型',
	source_id bigint  comment '来源编号',
		 primary key(id) not enforced
)
 WITH (
 'connector' = 'mysql-cdc',
 'hostname' = 'hadoop102',
 'port' = '3306',
 'username' = 'root',
 'password' = '000000',
 'database-name' = 'gmall2020',
 'table-name' = 'order_detail'
);
CREATE TABLE user_info
(
	id bigint comment '编号',
	login_name varchar(200)  comment '用户名称',
	nick_name varchar(200)  comment '用户昵称',
	passwd varchar(200)  comment '用户密码',
	name varchar(200)  comment '用户姓名',
	phone_num varchar(200)  comment '手机号',
	email varchar(200)  comment '邮箱',
	head_img varchar(200)  comment '头像',
	user_level varchar(200)  comment '用户级别',
	birthday date  comment '用户生日',
	gender varchar(1)  comment '性别 M男,F女',
	create_time TIMESTAMP comment '创建时间',
	operate_time TIMESTAMP comment '修改时间',
	 primary key(id) not enforced
)
 WITH (
 'connector' = 'mysql-cdc',
 'hostname' = 'hadoop102',
 'port' = '3306',
 'username' = 'root',
 'password' = '000000',
 'database-name' = 'gmall2020',
 'table-name' = 'user_info'
);
CREATE TABLE base_province
(
	id bigint  comment 'id',
	name varchar(20)  comment '省名称',
	region_id varchar(20)  comment '大区id',
	area_code varchar(20)  comment '行政区位码',
	iso_code varchar(20)  comment '国际编码',
    primary key(id) not enforced
)
WITH (
 'connector' = 'mysql-cdc',
 'hostname' = 'hadoop102',
 'port' = '3306',
 'username' = 'root',
 'password' = '000000',
 'database-name' = 'gmall2020',
 'table-name' = 'base_province'
);

CREATE TABLE first_order_index (
    id BIGINT,
    order_status STRING,
    user_id BIGINT,
    final_total_amount DECIMAL(16, 2),
    benefit_reduce_amount DECIMAL(16, 2),
    original_total_amount DECIMAL(16, 2),
    feight_fee DECIMAL(16, 2),
    expire_time TIMESTAMP,
    create_time TIMESTAMP,
    operate_time TIMESTAMP,
    create_date STRING,
    create_hour STRING,
    if_first_order BOOLEAN,
    province_name STRING,
    province_area_code STRING,
    province_iso_code STRING,
    user_age_group DATE,
    user_gender STRING,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
  'connector' = 'elasticsearch-6',
  'hosts' = 'http://hadoop102:9200；http://hadoop103:9200；http://hadoop104:9200',
  'index' = 'first_order_{create_date}',
  'document-type' = '_doc'
);
CREATE VIEW first_order_view
AS
SELECT  o.id,
        order_status,
        user_id,
        CAST(final_total_amount AS DOUBLE)   final_total_amount,
        CAST(benefit_reduce_amount AS DOUBLE)  benefit_reduce_amount,
        CAST(original_total_amount AS DOUBLE) original_total_amount,
        CAST(feight_fee AS DOUBLE) feight_fee,
        expire_time,
        o.create_time,
        o.operate_time,
        DATE_FORMAT(o.create_time, 'yyyyMMdd') create_date,
        DATE_FORMAT(o.create_time, 'HH') create_hour,
        true if_first_order,
        p.name province_name,
        p.area_code province_area_code,
        p.iso_code province_iso_code,
        u.birthday user_age_group,
        u.gender user_gender
FROM order_info o
LEFT JOIN user_info u on o.user_id = u.id
LEFT JOIN base_province p on o.province_id = p.id
;

