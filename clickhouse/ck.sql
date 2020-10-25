drop table if exists ck_test;
drop table if exists target;
create table ck_test(id   UInt64,name String) engine Kafka('localhost:9092', 'ck_test', 'ck.test.group', 'CSV');
create table target(id UInt64, name String) engine MergeTree primary key id order by id;
create materialized view mv_test to target AS select * from ck_test;
select * from ck_test;
select * from mv_test;
select * from target;