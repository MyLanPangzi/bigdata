drop table if exists ods_log;
CREATE EXTERNAL TABLE IF NOT EXISTS ods_log
(
    line string
)
    PARTITIONED BY (dt string)
    STORED AS INPUTFORMAT "com.hadoop.mapred.DeprecatedLzoTextInputFormat"
        OUTPUTFORMAT "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"
location '/warehouse/gmall/ods/ods_log';
-- log_to_ods.sh