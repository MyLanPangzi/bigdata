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
 'properties.bootstrap.servers' = 'hadoop102:9092;hadoop103:9092;hadoop104:9092',
 'properties.group.id' = 'testGroup',
 'format' = 'json',
 'scan.startup.mode' = 'earliest-offset'
)
;

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
  'hosts' = 'http://hadoop102:9200;http://hadoop103:9200;http://hadoop104:9200',
  'index' = 'dau_{dt}',
  'document-type' = '_doc'
)
;

CREATE TABLE redis_dau_table (
    mid STRING,
    dt STRING,
    PRIMARY KEY (mid) NOT ENFORCED
) WITH (
  'connector' = 'redis',
  'format' = 'json',
  'host' = 'hadoop102',
  'port' = '6379',
  'map-key' = 'dau_{dt}',
  'key-delimiter' = '_'
)
;
INSERT INTO redis_dau_table
SELECT
    common.mid,
    FROM_UNIXTIME(ts / 1000, 'yyyy-MM-dd') dt
FROM start_event s
;


INSERT INTO dau_index
SELECT
    common.mid,
    common.uid,
    common.ar,
    common.ch,
    common.vc,
    FROM_UNIXTIME(ts / 1000, 'yyyy-MM-dd') dt,
    FROM_UNIXTIME(ts / 1000, 'HH') hr,
    FROM_UNIXTIME(ts / 1000, 'mm') mi,
    ts
FROM start_event s
;
-- LEFT JOIN redis_start r FOR SYSTEM_TIME OF s.proctime ON s.common.uid = r.common.uid
-- WHERE r.common IS NULL AND r.common.uid IS NULL
