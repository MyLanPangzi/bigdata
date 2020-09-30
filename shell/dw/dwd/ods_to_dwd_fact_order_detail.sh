#!/usr/bin/env bash

day=$1
if [ -z "$day" ]; then
  day=$(date -d '-1 day' +%F)
fi

# 折后分摊=数量*金额/原始总金额 * 折后总金额
# 运费分摊=数量*金额/原始总金额 * 运费金额
# 优惠分摊=数量*金额/原始总金额 * 优惠金额
# 开窗函数的使用是为了计算精度丢失，原始总金额-累计各分摊总额=差异金额，
# 差异金额+分摊金额（编号为1），保证总额不变
app=gmall
sql="
insert overwrite table $app.dwd_fact_order_detail partition (dt = '$day')
select id,
       order_id,
       user_id,
       sku_id,
       sku_name,
       order_price,
       sku_num,
       create_time,
       source_type,
       source_id,
       province_id,
       original_amount_d,
       if(num = 1, final_total_amount - sum_final_amount_d + final_amount_d, final_amount_d),
       if(num = 1, feight_fee - sum_feight_fee_d + feight_fee_d, feight_fee_d),
       if(num = 1, benefit_reduce_amount - sum_benefit_reduce_amount_d + benefit_reduce_amount_d,
          benefit_reduce_amount_d)
from (
         select d.id,
                d.order_id,
                d.user_id,
                d.sku_id,
                d.sku_name,
                d.order_price,
                d.sku_num,
                d.create_time,
                d.source_type,
                d.source_id,
                o.province_id,
                o.final_total_amount,
                o.feight_fee,
                o.benefit_reduce_amount,
                round(d.sku_num * d.order_price / o.final_total_amount)                      original_amount_d,
                round(d.sku_num * d.order_price / o.original_total_amount * o.final_total_amount,
                      2)                                                                     final_amount_d,
                sum(round(d.sku_num * d.order_price / o.original_total_amount * o.final_total_amount, 2))
                    over (partition by order_id)                                             sum_final_amount_d,
                round(d.sku_num * d.order_price / o.original_total_amount * o.feight_fee, 2) feight_fee_d,
                sum(round(d.sku_num * d.order_price / o.original_total_amount * o.feight_fee, 2))
                    over (partition by order_id)                                             sum_feight_fee_d,
                round(d.sku_num * d.order_price / o.original_total_amount * o.benefit_reduce_amount, 2)
                                                                                             benefit_reduce_amount_d,
                sum(round(d.sku_num * d.order_price / o.original_total_amount * o.benefit_reduce_amount, 2))
                    over (partition by order_id)                                              sum_benefit_reduce_amount_d,
                row_number() over (partition by order_id)                                                         num
         from (
                  select id,
                         order_id,
                         user_id,
                         sku_id,
                         sku_name,
                         order_price,
                         sku_num,
                         create_time,
                         source_type,
                         source_id
                  from $app.ods_order_detail
                  where dt = '$day'
              ) d
                  inner join (
             select id,
                    province_id,
                    final_total_amount,
                    original_total_amount,
                    feight_fee,
                    benefit_reduce_amount
             from $app.ods_order_info
             where dt = '$day'
         ) o on o.id = d.order_id
     ) t;
"
echo "$sql"
