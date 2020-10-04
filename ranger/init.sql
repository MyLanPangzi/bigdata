create database ranger;
set global validate_password_length=4;
set global validate_password_policy=0;
grant all privileges on ranger.* to ranger@'%'  identified by 'ranger';