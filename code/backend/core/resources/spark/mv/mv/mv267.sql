CREATE MATERIALIZED VIEW IF NOT EXISTS mv267
PARTITIONED BY (ss_sold_date_sk)
AS
SELECT
	store_sales.ss_hdemo_sk,
	household_demographics.hd_vehicle_count,
	customer.c_birth_month,
	date_dim.d_qoy,
	store.s_store_name,
	customer_address.ca_city,
	store_sales.ss_list_price,
	store_sales.ss_store_sk,
	customer_address.ca_street_name,
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
	customer_address.ca_street_number,
	customer.c_birth_year,
	customer_address.ca_state,
	store.s_store_sk,
	household_demographics.hd_income_band_sk,
	store_sales.ss_ext_discount_amt,
	store_sales.ss_promo_sk,
	store_sales.ss_wholesale_cost,
	customer_address.ca_country,
	store.s_number_employees,
	customer_address.ca_suite_number,
	date_dim.d_dom,
	store.s_county,
	store.s_street_type,
	store_sales.ss_net_paid,
	store_sales.ss_quantity,
	customer.c_preferred_cust_flag,
	customer_address.ca_location_type,
	date_dim.d_month_seq,
	store.s_suite_number,
	customer_address.ca_street_type,
	store_sales.ss_ext_sales_price,
	date_dim.d_date_sk,
	household_demographics.hd_buy_potential,
	customer_address.ca_county,
	store_sales.ss_ticket_number,
	store.s_zip,
	store_sales.ss_cdemo_sk,
	date_dim.d_date,
	store.s_store_id,
	customer.c_current_addr_sk,
	customer.c_first_name,
	store.s_company_name,
	store.s_city,
	customer_address.ca_zip,
	store_sales.ss_ext_list_price,
	store.s_gmt_offset,
	date_dim.d_year,
	customer_address.ca_address_sk,
	store_sales.ss_net_profit,
	store_sales.ss_sold_date_sk,
	customer.c_first_shipto_date_sk,
	store_sales.ss_addr_sk,
	store.s_company_id,
	store_sales.ss_item_sk,
	store_sales.ss_coupon_amt,
	customer.c_customer_sk,
	store_sales.ss_sold_time_sk,
	customer_address.ca_gmt_offset,
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
	date_dim,
	store_sales,
	store,
	customer,
	customer_address,
	household_demographics
WHERE
	store_sales.ss_store_sk = store.s_store_sk
	AND store_sales.ss_customer_sk = customer.c_customer_sk
	AND store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
	AND store_sales.ss_sold_date_sk = date_dim.d_date_sk
	AND store_sales.ss_addr_sk = customer_address.ca_address_sk
	AND (((store.s_city = 'Midway') OR (store.s_city = 'Fairview')))
	AND customer.c_current_addr_sk = customer_address.ca_address_sk
	AND household_demographics.hd_vehicle_count = 3
	AND household_demographics.hd_dep_count = 4
	AND date_dim.d_dom >= 1
	AND date_dim.d_dom <= 2
	AND (((date_dim.d_year = 1999) OR (date_dim.d_year = 2000) OR (date_dim.d_year = 2001)))
DISTRIBUTE BY ss_sold_date_sk;