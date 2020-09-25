#!/usr/bin/env bash
ods_to_dwd.sh dwd_dim_base_province 2020-06-14
ods_to_dwd.sh dwd_dim_sku_info 2020-06-14
ods_to_dwd.sh dwd_dim_coupon_info 2020-06-14
ods_to_dwd.sh dwd_dim_activity_info 2020-06-14
ods_to_dwd.sh dwd_fact_payment_info 2020-06-14
ods_to_dwd.sh dwd_fact_refund_info 2020-06-14
ods_to_dwd.sh dwd_fact_comment_info 2020-06-14
ods_to_dwd.sh dwd_fact_cart_info 2020-06-14
ods_to_dwd.sh dwd_fact_favor_info 2020-06-14
ods_to_dwd.sh dwd_fact_order_detail 2020-06-14
ods_to_dwd.sh dwd_fact_coupon_use 2020-06-14
ods_to_dwd.sh dwd_fact_order_info 2020-06-14
ods_to_dwd.sh dwd_dim_user_info 2020-06-14

ods_to_dwd.sh first 2020-06-14
ods_to_dwd.sh all 2020-06-15

dwd_to_dws.sh dws_uv_detail_daycount 2020-06-14
dwd_to_dws.sh dws_user_action_daycount 2020-06-14
dwd_to_dws.sh dws_sku_action_daycount 2020-06-14

dws_to_dwt.sh dwt_uv_topic 2020-06-14
dws_to_dwt.sh dwt_user_topic 2020-06-14
dws_to_dwt.sh dwt_sku_topic 2020-06-14
