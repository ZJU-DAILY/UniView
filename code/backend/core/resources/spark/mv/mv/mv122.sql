CREATE MATERIALIZED VIEW IF NOT EXISTS mv122
PARTITIONED BY (ss_sold_date_sk)
AS
SELECT
	customer_demographics.cd_purchase_estimate,
	date_dim.d_month_seq,
	store.s_suite_number,
	store_sales.ss_hdemo_sk,
	store_sales.ss_ext_sales_price,
	customer_demographics.cd_dep_college_count,
	date_dim.d_date_sk,
	date_dim.d_qoy,
	store.s_store_name,
	customer_demographics.cd_dep_count,
	store_sales.ss_ticket_number,
	store.s_zip,
	store_sales.ss_cdemo_sk,
	date_dim.d_date,
	store_sales.ss_list_price,
	store.s_store_id,
	store_sales.ss_store_sk,
	customer_demographics.cd_education_status,
	store_sales.ss_ext_wholesale_cost,
	store.s_company_name,
	store.s_street_name,
	customer_demographics.cd_demo_sk,
	store.s_city,
	store.s_market_id,
	date_dim.d_moy,
	store_sales.ss_customer_sk,
	date_dim.d_week_seq,
	store_sales.ss_ext_list_price,
	store.s_gmt_offset,
	date_dim.d_year,
	store.s_state,
	store_sales.ss_net_profit,
	store_sales.ss_sold_date_sk,
	customer_demographics.cd_gender,
	store_sales.ss_addr_sk,
	date_dim.d_quarter_name,
	store_sales.ss_sales_price,
	store_sales.ss_item_sk,
	store_sales.ss_coupon_amt,
	customer_demographics.cd_marital_status,
	store.s_company_id,
	store_sales.ss_sold_time_sk,
	customer_demographics.cd_dep_employed_count,
	customer_demographics.cd_credit_rating,
	store.s_store_sk,
	store_sales.ss_ext_discount_amt,
	store_sales.ss_promo_sk,
	store_sales.ss_wholesale_cost,
	store.s_number_employees,
	store.s_street_number,
	date_dim.d_day_name,
	date_dim.d_dom,
	store.s_county,
	store_sales.ss_ext_tax,
	store.s_street_type,
	store_sales.ss_net_paid,
	store_sales.ss_quantity,
	date_dim.d_dow
FROM
	date_dim,
	store_sales,
	store,
	customer_demographics
WHERE
	store_sales.ss_store_sk = store.s_store_sk
	AND store_sales.ss_sold_date_sk = date_dim.d_date_sk
	AND store_sales.ss_cdemo_sk = customer_demographics.cd_demo_sk
	AND store.s_state = 'TN'
	AND date_dim.d_year = 2002
	AND customer_demographics.cd_marital_status = 'S'
	AND customer_demographics.cd_gender = 'M'
	AND customer_demographics.cd_education_status = 'College'
DISTRIBUTE BY ss_sold_date_sk;