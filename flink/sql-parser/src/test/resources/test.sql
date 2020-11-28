SET test=1;
CREATE TABLE kafkaTable (
 user_id BIGINT,
 item_id BIGINT,
 category_id BIGINT,
 behavior STRING,
 ts TIMESTAMP(3)
) WITH (
 'connector' = 'kafka',
 'topic' = 'user_behavior',
 'properties.bootstrap.servers' = 'localhost:9092',
 'properties.group.id' = 'testGroup',
 'format' = 'csv',
 'scan.startup.mode' = 'earliest-offset'
);
SELECT * FROM kafkaTable;

CREATE TABLE print (
 user_id BIGINT,
 item_id BIGINT,
 category_id BIGINT,
 behavior STRING,
 ts TIMESTAMP(3)
) WITH (
 'connector' = 'print'
);
INSERT INTO print
SELECT * FROM kafkaTable;
