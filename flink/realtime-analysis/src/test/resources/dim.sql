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
CREATE TABLE base_region
(
	id varchar(20)  comment '大区id',
	region_name varchar(20)  comment '大区名称',
    primary key(id) not enforced
)
WITH (
 'connector' = 'mysql-cdc',
 'hostname' = 'hadoop102',
 'port' = '3306',
 'username' = 'root',
 'password' = '000000',
 'database-name' = 'gmall2020',
 'table-name' = 'base_region'
);

CREATE TABLE base_trademark
(
	tm_id varchar(20)  comment '品牌id',
	tm_name varchar(20)  comment '品牌名称',
    primary key(tm_id) not enforced
)
WITH (
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
	id bigint  comment '编号',
	name varchar(10)  comment '分类名称',
    primary key(id) not enforced
)
WITH (
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
	id bigint  comment '编号',
	name varchar(200)   comment '二级分类名称',
	category1_id bigint  comment '一级分类编号',
    primary key(id) not enforced
)
WITH (
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
	id bigint comment '编号',
	name varchar(200)   comment '三级分类名称',
	category2_id bigint  comment '二级分类编号',
    primary key(id) not enforced
)
WITH (
 'connector' = 'mysql-cdc',
 'hostname' = 'hadoop102',
 'port' = '3306',
 'username' = 'root',
 'password' = '000000',
 'database-name' = 'gmall2020',
 'table-name' = 'base_category3'
);
CREATE TABLE sku_info
(
	id bigint  comment 'skuid(itemID)',
	spu_id bigint  comment 'spuid',
	price decimal  comment '价格',
	sku_name varchar(200)  comment 'sku名称',
	sku_desc varchar(2000)  comment '商品规格描述',
	weight decimal(10,2)  comment '重量',
	tm_id bigint  comment '品牌(冗余)',
	category3_id bigint  comment '三级分类id（冗余)',
	sku_default_img varchar(200)  comment '默认显示图片(冗余)',
	create_time TIMESTAMP  comment '创建时间',
    primary key(id) not enforced
)
WITH (
 'connector' = 'mysql-cdc',
 'hostname' = 'hadoop102',
 'port' = '3306',
 'username' = 'root',
 'password' = '000000',
 'database-name' = 'gmall2020',
 'table-name' = 'sku_info'
);
CREATE TABLE spu_info
(
	id bigint comment '商品id',
	spu_name varchar(200)  comment '商品名称',
	description varchar(1000)  comment '商品描述(后台简述）',
	category3_id bigint  comment '三级分类id',
	tm_id bigint  comment '品牌id',
    primary key(id) not enforced
)
WITH (
 'connector' = 'mysql-cdc',
 'hostname' = 'hadoop102',
 'port' = '3306',
 'username' = 'root',
 'password' = '000000',
 'database-name' = 'gmall2020',
 'table-name' = 'spu_info'
);