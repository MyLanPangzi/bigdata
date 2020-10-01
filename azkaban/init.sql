create database azkaban;
set global validate_password_length =4;
set global validate_password_policy=0;
CREATE USER 'azkaban'@'%' IDENTIFIED BY '000000';
GRANT SELECT,INSERT,UPDATE,DELETE ON azkaban.* to 'azkaban'@'%' WITH GRANT OPTION;

# source /opt/module/azkaban-db/create-all-sql-3.84.4.sql
# sudo vim /etc/my.cnf
# [mysqld]
# max_allowed_packet=1024M