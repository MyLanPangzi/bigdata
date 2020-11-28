CREATE TABLE start_event (
 common ROW(
    ar STRING,
    ba STRING,
    ch STRING,
    md STRING,
    mid  STRING,
    os STRING,
    uid  STRING,
    vc STRING
 ),
 `start` ROW(
    entry STRING,
    loading_time STRING,
    open_ad_id STRING,
    open_ad_ms STRING,
    open_ad_skip_ms STRING
 ),
 ts BIGINT
) WITH (
 'connector' = 'kafka',
 'topic' = 'start-topic',
 'properties.bootstrap.servers' = 'hadoop102:9092；hadoop103:9092；hadoop104:9092',
 'properties.group.id' = 'testGroup',
 'format' = 'json',
 'scan.startup.mode' = 'earliest-offset'
);

CREATE TABLE dau_index (
    mid STRING,
    uid STRING,
    ar STRING,
    ch STRING,
    vc STRING,
    dt STRING,
    hr STRING,
    mi STRING,
    ts BIGINT,
    PRIMARY KEY (mid) NOT ENFORCED
) WITH (
  'connector' = 'elasticsearch-6',
  'hosts' = 'http://hadoop102:9200；http://hadoop103:9200；http://hadoop104:9200',
  'index' = 'dau_{dt}',
  'document-type' = '_doc'
);