CREATE MATERIALIZED VIEW IF NOT EXISTS mv143
PARTITIONED BY (ss_sold_date_sk)
AS
SELECT
	store_sales.ss_hdemo_sk,
	household_demographics.hd_vehicle_count,
	customer.c_birth_month,
	date_dim.d_qoy,
	store.s_store_name,
	store_sales.ss_list_price,
	store_sales.ss_store_sk,
	store_sales.ss_ext_wholesale_cost,
	customer.c_birth_country,
	customer.c_last_review_date_sk,
	store.s_street_name,
	store.s_market_id,
	date_dim.d_moy,
	store_sales.ss_customer_sk,
	date_dim.d_week_seq,
	household_demographics.hd_dep_count,
	customer.c_current_cdemo_sk,
	customer.c_last_name,
	store.s_state,
	customer.c_birth_day,
	customer.c_email_address,
	household_demographics.hd_demo_sk,
	date_dim.d_quarter_name,
	store_sales.ss_sales_price,
	customer.c_birth_year,
	store.s_store_sk,
	household_demographics.hd_income_band_sk,
	store_sales.ss_ext_discount_amt,
	store_sales.ss_promo_sk,
	store_sales.ss_wholesale_cost,
	store.s_number_employees,
	date_dim.d_dom,
	store.s_county,
	store.s_street_type,
	store_sales.ss_net_paid,
	store_sales.ss_quantity,
	customer.c_preferred_cust_flag,
	date_dim.d_month_seq,
	store.s_suite_number,
	store_sales.ss_ext_sales_price,
	date_dim.d_date_sk,
	household_demographics.hd_buy_potential,
	store_sales.ss_ticket_number,
	store.s_zip,
	store_sales.ss_cdemo_sk,
	date_dim.d_date,
	store.s_store_id,
	customer.c_current_addr_sk,
	customer.c_first_name,
	store.s_company_name,
	store.s_city,
	store_sales.ss_ext_list_price,
	store.s_gmt_offset,
	date_dim.d_year,
	store_sales.ss_net_profit,
	store_sales.ss_sold_date_sk,
	customer.c_first_shipto_date_sk,
	store_sales.ss_addr_sk,
	store.s_company_id,
	store_sales.ss_item_sk,
	store_sales.ss_coupon_amt,
	customer.c_customer_sk,
	store_sales.ss_sold_time_sk,
	customer.c_customer_id,
	customer.c_login,
	customer.c_first_sales_date_sk,
	store.s_street_number,
	date_dim.d_day_name,
	store_sales.ss_ext_tax,
	customer.c_salutation,
	date_dim.d_dow,
	customer.c_current_hdemo_sk
FROM
	customer,
	date_dim,
	store_sales,
	store,
	household_demographics
WHERE
	store_sales.ss_store_sk = store.s_store_sk
	AND store_sales.ss_customer_sk = customer.c_customer_sk
	AND store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
	AND store_sales.ss_sold_date_sk = date_dim.d_date_sk
	AND store.s_county = 'Williamson County'
	AND (((date_dim.d_year = 1999) OR (date_dim.d_year = 2000) OR (date_dim.d_year = 2001)))
	AND date_dim.d_dom >= 1
	AND date_dim.d_dom <= 28
	AND household_demographics.hd_vehicle_count > 0
	AND (((household_demographics.hd_buy_potential = '>10000') OR (household_demographics.hd_buy_potential = 'unknown')))
DISTRIBUTE BY ss_sold_date_sk;