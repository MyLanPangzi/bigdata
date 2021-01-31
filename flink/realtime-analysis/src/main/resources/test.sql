CREATE TABLE datagen
(
    id      BIGINT PRIMARY KEY NOT ENFORCED,
    name    STRING,
    `year`  INT,
    `month` INT,
    `day`   INT,
    ts      TIMESTAMP
) WITH(
    'connector' = 'datagen',
    'fields.id.min' = '1',
    'fields.id.max' = '999999999',
    'fields.name.length' = '10',
    'fields.year.min' = '2000',
    'fields.year.max' = '2010',
    'fields.month.min' = '1',
    'fields.month.max' = '12',
    'fields.day.min' = '1',
    'fields.day.max' = '31',
    'rows-per-second' = '2'
);
-- CREATE TABLE print WITH( 'connector' = 'print' )
-- LIKE student ( EXCLUDING ALL);

CREATE TABLE mysql_student
(
    id          BIGINT PRIMARY KEY NOT ENFORCED ,
    name        VARCHAR(20),
    create_time TIMESTAMP(3)
) WITH (
 'connector' = 'mysql-cdc',
 'server-time-zone' = 'Asia/Shanghai',
 'hostname' = 'hdp10',
 'port' = '3306',
 'username' = 'root',
 'password' = 'Hdp10Hdp10@',
 'database-name' = 'flink',
 'table-name' = 'student'
);

CREATE TABLE hudi
(
    id      BIGINT PRIMARY KEY NOT ENFORCED,
    name    STRING,
    create_time TIMESTAMP(3),
    `year`  BIGINT,
    `month` BIGINT,
    `day`   BIGINT
) PARTITIONED BY (`year`,`month`,`day`) WITH (
    'connector' = 'hudi',
    'base-path' = 'hdfs://hdp10:8020/student',
    'table-name' = 'student',
    'precombine-field-prop' = 'create_time',
    'format' = 'json',
    'schema' = '{
              "type": "record",
              "name": "test",
              "fields": [
                {
                  "name": "id",
                  "type": "long"
                },
                {
                  "name": "name",
                  "type": "string"
                },
                {
                  "name": "create_time",
                  "type": "string"
                }
              ]
            }'
);

-- INSERT INTO print SELECT * FROM student;
-- INSERT INTO print SELECT * FROM datagen;
-- INSERT INTO hudi SELECT * FROM datagen;
INSERT INTO hudi SELECT id, name, create_time,YEAR(create_time),MONTH(create_time),DAYOFMONTH(create_time) FROM mysql_student;
