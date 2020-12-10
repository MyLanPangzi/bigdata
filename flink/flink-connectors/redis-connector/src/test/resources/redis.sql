CREATE TABLE redis (
  id BIGINT,
  status BOOLEAN,
  PRIMARY KEY (id) NOT ENFORCED
) WITH (
   'connector' = 'redis',
   'format' = 'json',
   'hostname' = 'hadoop102',
   'port' = '6379',
   'hash-key' = 'test',
   'key-delimiter' = '_',
   'ttl' = '1000'
);
