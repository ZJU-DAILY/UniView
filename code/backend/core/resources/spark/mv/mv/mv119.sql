CREATE MATERIALIZED VIEW IF NOT EXISTS mv119
PARTITIONED BY (cs_sold_date_sk)
AS
SELECT
	customer_demographics.cd_purchase_estimate,
	date_dim.d_month_seq,
	catalog_sales.cs_ship_customer_sk,
	customer_demographics.cd_dep_college_count,
	catalog_sales.cs_ext_ship_cost,
	date_dim.d_date_sk,
	catalog_sales.cs_catalog_page_sk,
	date_dim.d_qoy,
	catalog_sales.cs_call_center_sk,
	customer_demographics.cd_dep_count,
	date_dim.d_date,
	catalog_sales.cs_bill_cdemo_sk,
	catalog_sales.cs_order_number,
	catalog_sales.cs_ext_sales_price,
	customer_demographics.cd_education_status,
	catalog_sales.cs_wholesale_cost,
	catalog_sales.cs_bill_addr_sk,
	catalog_sales.cs_net_paid,
	customer_demographics.cd_demo_sk,
	catalog_sales.cs_ext_wholesale_cost,
	catalog_sales.cs_ship_date_sk,
	date_dim.d_moy,
	date_dim.d_week_seq,
	date_dim.d_year,
	catalog_sales.cs_promo_sk,
	catalog_sales.cs_item_sk,
	catalog_sales.cs_ship_mode_sk,
	catalog_sales.cs_bill_customer_sk,
	catalog_sales.cs_ext_list_price,
	customer_demographics.cd_gender,
	date_dim.d_quarter_name,
	catalog_sales.cs_bill_hdemo_sk,
	customer_demographics.cd_marital_status,
	catalog_sales.cs_warehouse_sk,
	customer_demographics.cd_dep_employed_count,
	customer_demographics.cd_credit_rating,
	catalog_sales.cs_sold_time_sk,
	catalog_sales.cs_sales_price,
	catalog_sales.cs_quantity,
	catalog_sales.cs_coupon_amt,
	catalog_sales.cs_list_price,
	catalog_sales.cs_net_profit,
	catalog_sales.cs_ship_addr_sk,
	date_dim.d_day_name,
	date_dim.d_dom,
	catalog_sales.cs_net_paid_inc_tax,
	catalog_sales.cs_ext_discount_amt,
	date_dim.d_dow,
	catalog_sales.cs_sold_date_sk
FROM
	date_dim,
	catalog_sales,
	customer_demographics
WHERE
	catalog_sales.cs_bill_cdemo_sk = customer_demographics.cd_demo_sk
	AND catalog_sales.cs_sold_date_sk = date_dim.d_date_sk
	AND customer_demographics.cd_gender = 'M'
	AND customer_demographics.cd_education_status = 'College'
	AND customer_demographics.cd_marital_status = 'S'
	AND date_dim.d_year = 2000
DISTRIBUTE BY cs_sold_date_sk;