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