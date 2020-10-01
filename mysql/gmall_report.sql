create database `gmall_report` character set 'utf8' collate 'utf8_general_ci';
DROP TABLE IF EXISTS `ads_user_topic`;
CREATE TABLE `ads_user_topic`
(
    `dt`                    date           NOT NULL,
    `day_users`             bigint(255)    NULL DEFAULT NULL,
    `day_new_users`         bigint(255)    NULL DEFAULT NULL,
    `day_new_payment_users` bigint(255)    NULL DEFAULT NULL,
    `payment_users`         bigint(255)    NULL DEFAULT NULL,
    `users`                 bigint(255)    NULL DEFAULT NULL,
    `day_users2users`       double(255, 2) NULL DEFAULT NULL,
    `payment_users2users`   double(255, 2) NULL DEFAULT NULL,
    `day_new_users2users`   double(255, 2) NULL DEFAULT NULL,
    PRIMARY KEY (`dt`) USING BTREE
) ENGINE = InnoDB
  CHARACTER SET = utf8
  COLLATE = utf8_general_ci
  ROW_FORMAT = Compact;
DROP TABLE IF EXISTS `ads_area_topic`;
CREATE TABLE `ads_area_topic`
(
    `dt`                 date                                                    NOT NULL,
    `id`                 int(11)                                                 NULL DEFAULT NULL,
    `province_name`      varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
    `area_code`          varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
    `iso_code`           varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
    `region_id`          int(11)                                                 NULL DEFAULT NULL,
    `region_name`        varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
    `login_day_count`    bigint(255)                                             NULL DEFAULT NULL,
    `order_day_count`    bigint(255)                                             NULL DEFAULT NULL,
    `order_day_amount`   double(255, 2)                                          NULL DEFAULT NULL,
    `payment_day_count`  bigint(255)                                             NULL DEFAULT NULL,
    `payment_day_amount` double(255, 2)                                          NULL DEFAULT NULL,
    PRIMARY KEY (`dt`, `iso_code`) USING BTREE
) ENGINE = InnoDB
  CHARACTER SET = utf8
  COLLATE = utf8_general_ci
  ROW_FORMAT = Compact;

