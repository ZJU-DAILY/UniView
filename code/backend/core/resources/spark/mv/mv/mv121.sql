CREATE MATERIALIZED VIEW IF NOT EXISTS mv121
PARTITIONED BY (cs_sold_date_sk)
AS
SELECT
	customer_demographics.cd_purchase_estimate,
	item.i_brand,
	date_dim.d_month_seq,
	catalog_sales.cs_ship_customer_sk,
	promotion.p_channel_tv,
	customer_demographics.cd_dep_college_count,
	catalog_sales.cs_ext_ship_cost,
	date_dim.d_date_sk,
	catalog_sales.cs_catalog_page_sk,
	date_dim.d_qoy,
	item.i_brand_id,
	customer_demographics.cd_dep_count,
	catalog_sales.cs_call_center_sk,
	promotion.p_channel_email,
	date_dim.d_date,
	item.i_manager_id,
	item.i_class_id,
	item.i_item_desc,
	catalog_sales.cs_bill_cdemo_sk,
	catalog_sales.cs_order_number,
	catalog_sales.cs_ext_sales_price,
	customer_demographics.cd_education_status,
	catalog_sales.cs_wholesale_cost,
	promotion.p_channel_event,
	item.i_class,
	item.i_category,
	catalog_sales.cs_bill_addr_sk,
	catalog_sales.cs_net_paid,
	customer_demographics.cd_demo_sk,
	item.i_size,
	catalog_sales.cs_ext_wholesale_cost,
	catalog_sales.cs_ship_date_sk,
	item.i_item_id,
	date_dim.d_moy,
	item.i_current_price,
	date_dim.d_week_seq,
	date_dim.d_year,
	catalog_sales.cs_promo_sk,
	catalog_sales.cs_item_sk,
	catalog_sales.cs_ship_mode_sk,
	catalog_sales.cs_bill_customer_sk,
	catalog_sales.cs_ext_list_price,
	item.i_manufact,
	item.i_units,
	promotion.p_promo_sk,
	customer_demographics.cd_gender,
	date_dim.d_quarter_name,
	catalog_sales.cs_bill_hdemo_sk,
	customer_demographics.cd_marital_status,
	item.i_item_sk,
	item.i_product_name,
	customer_demographics.cd_dep_employed_count,
	catalog_sales.cs_warehouse_sk,
	item.i_wholesale_cost,
	customer_demographics.cd_credit_rating,
	catalog_sales.cs_sold_time_sk,
	item.i_color,
	catalog_sales.cs_sales_price,
	catalog_sales.cs_quantity,
	catalog_sales.cs_coupon_amt,
	catalog_sales.cs_list_price,
	catalog_sales.cs_net_profit,
	catalog_sales.cs_ship_addr_sk,
	date_dim.d_day_name,
	item.i_category_id,
	date_dim.d_dom,
	item.i_manufact_id,
	catalog_sales.cs_net_paid_inc_tax,
	catalog_sales.cs_ext_discount_amt,
	date_dim.d_dow,
	promotion.p_channel_dmail,
	catalog_sales.cs_sold_date_sk
FROM
	date_dim,
	customer_demographics,
	promotion,
	catalog_sales,
	item
WHERE
	catalog_sales.cs_bill_cdemo_sk = customer_demographics.cd_demo_sk
	AND catalog_sales.cs_promo_sk = promotion.p_promo_sk
	AND catalog_sales.cs_item_sk = item.i_item_sk
	AND catalog_sales.cs_sold_date_sk = date_dim.d_date_sk
	AND customer_demographics.cd_education_status = 'College'
	AND customer_demographics.cd_marital_status = 'S'
	AND customer_demographics.cd_gender = 'M'
	AND date_dim.d_year = 2000
	AND promotion.p_channel_event = 'N'
	AND promotion.p_channel_email = 'N'
DISTRIBUTE BY cs_sold_date_sk;