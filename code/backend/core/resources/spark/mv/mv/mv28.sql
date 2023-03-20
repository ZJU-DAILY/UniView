CREATE MATERIALIZED VIEW IF NOT EXISTS mv28
PARTITIONED BY (ss_sold_date_sk)
AS
SELECT
	item.i_brand,
	customer_demographics.cd_purchase_estimate,
	date_dim.d_month_seq,
	promotion.p_channel_tv,
	store_sales.ss_hdemo_sk,
	store_sales.ss_ext_sales_price,
	customer_demographics.cd_dep_college_count,
	item.i_brand_id,
	date_dim.d_date_sk,
	date_dim.d_qoy,
	customer_demographics.cd_dep_count,
	promotion.p_channel_email,
	store_sales.ss_ticket_number,
	store_sales.ss_cdemo_sk,
	date_dim.d_date,
	store_sales.ss_list_price,
	item.i_manager_id,
	item.i_class_id,
	store_sales.ss_store_sk,
	item.i_item_desc,
	item.i_class,
	customer_demographics.cd_education_status,
	store_sales.ss_ext_wholesale_cost,
	promotion.p_channel_event,
	item.i_category,
	customer_demographics.cd_demo_sk,
	item.i_size,
	item.i_item_id,
	date_dim.d_moy,
	item.i_current_price,
	store_sales.ss_ext_list_price,
	date_dim.d_week_seq,
	store_sales.ss_customer_sk,
	date_dim.d_year,
	item.i_manufact,
	store_sales.ss_net_profit,
	item.i_units,
	store_sales.ss_sold_date_sk,
	promotion.p_promo_sk,
	customer_demographics.cd_gender,
	store_sales.ss_addr_sk,
	date_dim.d_quarter_name,
	store_sales.ss_sales_price,
	store_sales.ss_item_sk,
	store_sales.ss_coupon_amt,
	customer_demographics.cd_marital_status,
	item.i_item_sk,
	store_sales.ss_sold_time_sk,
	item.i_product_name,
	item.i_wholesale_cost,
	customer_demographics.cd_dep_employed_count,
	customer_demographics.cd_credit_rating,
	store_sales.ss_ext_discount_amt,
	store_sales.ss_promo_sk,
	item.i_color,
	store_sales.ss_wholesale_cost,
	date_dim.d_day_name,
	item.i_category_id,
	date_dim.d_dom,
	store_sales.ss_ext_tax,
	item.i_manufact_id,
	promotion.p_channel_dmail,
	store_sales.ss_net_paid,
	store_sales.ss_quantity,
	date_dim.d_dow
FROM
	date_dim,
	store_sales,
	item,
	promotion,
	customer_demographics
WHERE
	store_sales.ss_item_sk = item.i_item_sk
	AND store_sales.ss_promo_sk = promotion.p_promo_sk
	AND store_sales.ss_sold_date_sk = date_dim.d_date_sk
	AND store_sales.ss_cdemo_sk = customer_demographics.cd_demo_sk
	AND promotion.p_channel_event = 'N'
	AND promotion.p_channel_email = 'N'
	AND date_dim.d_year = 2000
	AND customer_demographics.cd_marital_status = 'S'
	AND customer_demographics.cd_gender = 'M'
	AND customer_demographics.cd_education_status = 'College'
DISTRIBUTE BY ss_sold_date_sk;