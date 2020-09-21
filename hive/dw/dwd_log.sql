drop table if exists dwd_start_log;
create external table dwd_start_log
(
    `area_code`       string comment '地区编码',
    `brand`           string comment '手机品牌',
    `channel`         string comment '渠道',
    `model`           string comment '手机型号',
    `mid_id`          string comment '设备id',
    `os`              string comment '操作系统',
    `user_id`         string comment '会员id',
    `version_code`    string comment 'app版本号',
    `entry`           string comment ' icon手机图标  notice 通知   install 安装后启动',
    `loading_time`    bigint comment '启动加载时间',
    `open_ad_id`      string comment '广告页id ',
    `open_ad_ms`      bigint comment '广告总共播放时间',
    `open_ad_skip_ms` bigint comment '用户跳过广告时点',
    `ts`              bigint comment '时间'
) comment '启动日志表'
    partitioned by (dt string) -- 按照时间创建分区
    stored as parquet -- 采用parquet列式存储
    location '/warehouse/gmall/dwd/dwd_start_log' -- 指定在hdfs上存储位置
    tblproperties ('parquet.compression' = 'lzo');
drop table if exists dwd_page_log;
create external table dwd_page_log
(
    `area_code`      string comment '地区编码',
    `brand`          string comment '手机品牌',
    `channel`        string comment '渠道',
    `model`          string comment '手机型号',
    `mid_id`         string comment '设备id',
    `os`             string comment '操作系统',
    `user_id`        string comment '会员id',
    `version_code`   string comment 'app版本号',
    `during_time`    bigint comment '持续时间毫秒',
    `page_item`      string comment '目标id ',
    `page_item_type` string comment '目标类型',
    `last_page_id`   string comment '上页类型',
    `page_id`        string comment '页面id ',
    `source_type`    string comment '来源类型',
    `ts`             bigint
) comment '页面日志表'
    partitioned by (dt string)
    stored as parquet
    location '/warehouse/gmall/dwd/dwd_page_log'
    tblproperties ('parquet.compression' = 'lzo');
drop table if exists dwd_action_log;
create external table dwd_action_log
(
    `area_code`      string comment '地区编码',
    `brand`          string comment '手机品牌',
    `channel`        string comment '渠道',
    `model`          string comment '手机型号',
    `mid_id`         string comment '设备id',
    `os`             string comment '操作系统',
    `user_id`        string comment '会员id',
    `version_code`   string comment 'app版本号',
    `during_time`    bigint comment '持续时间毫秒',
    `page_item`      string comment '目标id ',
    `page_item_type` string comment '目标类型',
    `last_page_id`   string comment '上页类型',
    `page_id`        string comment '页面id ',
    `source_type`    string comment '来源类型',
    `action_id`      string comment '动作id',
    `item`           string comment '目标id ',
    `item_type`      string comment '目标类型',
    `ts`             bigint comment '时间'
) comment '动作日志表'
    partitioned by (dt string)
    stored as parquet
    location '/warehouse/gmall/dwd/dwd_action_log'
    tblproperties ('parquet.compression' = 'lzo');
drop table if exists dwd_display_log;
create external table dwd_display_log
(
    `area_code`      string comment '地区编码',
    `brand`          string comment '手机品牌',
    `channel`        string comment '渠道',
    `model`          string comment '手机型号',
    `mid_id`         string comment '设备id',
    `os`             string comment '操作系统',
    `user_id`        string comment '会员id',
    `version_code`   string comment 'app版本号',
    `during_time`    bigint comment 'app版本号',
    `page_item`      string comment '目标id ',
    `page_item_type` string comment '目标类型',
    `last_page_id`   string comment '上页类型',
    `page_id`        string comment '页面id ',
    `source_type`    string comment '来源类型',
    `display_type`   string comment '曝光类型',
    `item`           string comment '曝光对象id ',
    `item_type`      string comment 'app版本号',
    `order`          bigint comment '出现顺序',
    `ts`             bigint comment 'app版本号'
) comment '曝光日志表'
    partitioned by (dt string)
    stored as parquet
    location '/warehouse/gmall/dwd/dwd_display_log'
    tblproperties ('parquet.compression' = 'lzo');
drop table if exists dwd_error_log;
create external table dwd_error_log
(
    `area_code`       string comment '地区编码',
    `brand`           string comment '手机品牌',
    `channel`         string comment '渠道',
    `model`           string comment '手机型号',
    `mid_id`          string comment '设备id',
    `os`              string comment '操作系统',
    `user_id`         string comment '会员id',
    `version_code`    string comment 'app版本号',
    `page_item`       string comment '目标id ',
    `page_item_type`  string comment '目标类型',
    `last_page_id`    string comment '上页类型',
    `page_id`         string comment '页面id ',
    `source_type`     string comment '来源类型',
    `entry`           string comment ' icon手机图标  notice 通知 install 安装后启动',
    `loading_time`    string comment '启动加载时间',
    `open_ad_id`      string comment '广告页id ',
    `open_ad_ms`      string comment '广告总共播放时间',
    `open_ad_skip_ms` string comment '用户跳过广告时点',
    `actions`         string comment '动作',
    `displays`        string comment '曝光',
    `error_code`      string comment '错误码',
    `msg`             string comment '错误信息',
    `ts`              string comment '时间'
) comment '错误日志表'
    partitioned by (dt string)
    stored as parquet
    location '/warehouse/gmall/dwd/dwd_error_log'
    tblproperties ('parquet.compression' = 'lzo');
