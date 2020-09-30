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
dwd_to_dws.sh dws_area_stats_daycount 2020-06-14
dwd_to_dws.sh dws_activity_info_daycount 2020-06-14

dws_to_dwt.sh dwt_uv_topic 2020-06-14
dws_to_dwt.sh dwt_user_topic 2020-06-14
dws_to_dwt.sh dwt_sku_topic 2020-06-15
dws_to_dwt.sh dwt_area_topic 2020-06-15
dws_to_dwt.sh dwt_activity_topic 2020-06-15

ads.sh all 2020-06-14
ads.sh ads_appraise_bad_topN 2020-06-14
ads.sh ads_area_topic 2020-06-14
ads.sh ads_back_count 2020-06-14
ads.sh ads_continuity_uv_count 2020-06-14
ads.sh ads_continuity_wk_count 2020-06-14
ads.sh ads_new_mid_count 2020-06-14
ads.sh ads_order_daycount 2020-06-14
ads.sh ads_payment_daycount 2020-06-14
ads.sh ads_product_cart_topN 2020-06-14
ads.sh ads_product_favor_topN 2020-06-14
ads.sh ads_product_info 2020-06-14
ads.sh ads_product_refund_topN 2020-06-14
ads.sh ads_product_sale_topN 2020-06-14
ads.sh ads_sale_tm_category1_stat_mn 2020-06-14
ads.sh ads_silent_count 2020-06-14
ads.sh ads_user_action_convert_day 2020-06-14
ads.sh ads_user_retention_day_rate 2020-06-14
ads.sh ads_user_topic 2020-06-14
ads.sh ads_uv_count 2020-06-14

