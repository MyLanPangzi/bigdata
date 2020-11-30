CREATE TABLE first_order_user_status (
  id BIGINT,
  status BOOLEAN,
  PRIMARY KEY (id) NOT ENFORCED
) WITH (
   'connector' = 'jdbc-extended',
   'url' = 'jdbc:phoenix:hadoop102',
   'driver' = 'org.apache.phoenix.jdbc.PhoenixDriver',
   'table-name' = 'USER_ORDER_STATUS',
   'enable-upsert' = 'true'
);