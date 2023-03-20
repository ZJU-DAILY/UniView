CREATE MATERIALIZED VIEW IF NOT EXISTS mv224
PARTITIONED BY (ss_sold_date_sk)
AS
SELECT
	date_dim.d_month_seq,
	store.s_suite_number,
	promotion.p_channel_tv,
	store.s_street_type,
	store_sales.ss_hdemo_sk,
	store_sales.ss_ext_sales_price,
	date_dim.d_date_sk,
	customer.c_birth_month,
	date_dim.d_qoy,
	store.s_store_name,
	promotion.p_channel_email,
	store_sales.ss_ticket_number,
	store.s_zip,
	store_sales.ss_cdemo_sk,
	date_dim.d_date,
	store_sales.ss_list_price,
	store.s_store_id,
	store_sales.ss_store_sk,
	store_sales.ss_ext_wholesale_cost,
	customer.c_current_addr_sk,
	customer.c_birth_country,
	promotion.p_channel_event,
	customer.c_first_name,
	customer.c_last_review_date_sk,
	store.s_company_name,
	store.s_street_name,
	store.s_city,
	store.s_market_id,
	date_dim.d_moy,
	store_sales.ss_customer_sk,
	date_dim.d_week_seq,
	store.s_gmt_offset,
	customer.c_current_cdemo_sk,
	store_sales.ss_ext_list_price,
	date_dim.d_year,
	customer.c_last_name,
	store.s_state,
	store_sales.ss_net_profit,
	customer.c_birth_day,
	customer.c_email_address,
	store_sales.ss_sold_date_sk,
	promotion.p_promo_sk,
	customer.c_first_shipto_date_sk,
	store_sales.ss_addr_sk,
	date_dim.d_quarter_name,
	store_sales.ss_sales_price,
	store_sales.ss_item_sk,
	store_sales.ss_coupon_amt,
	customer.c_customer_sk,
	store.s_company_id,
	store_sales.ss_sold_time_sk,
	customer.c_birth_year,
	store.s_store_sk,
	store_sales.ss_ext_discount_amt,
	customer.c_customer_id,
	store_sales.ss_promo_sk,
	customer.c_login,
	store_sales.ss_wholesale_cost,
	store.s_number_employees,
	customer.c_first_sales_date_sk,
	store.s_street_number,
	date_dim.d_day_name,
	date_dim.d_dom,
	store.s_county,
	store_sales.ss_ext_tax,
	customer.c_salutation,
	promotion.p_channel_dmail,
	store_sales.ss_net_paid,
	store_sales.ss_quantity,
	date_dim.d_dow,
	customer.c_preferred_cust_flag,
	customer.c_current_hdemo_sk
FROM
	customer,
	promotion,
	date_dim,
	store_sales,
	store
WHERE
	store_sales.ss_store_sk = store.s_store_sk
	AND store_sales.ss_customer_sk = customer.c_customer_sk
	AND store_sales.ss_promo_sk = promotion.p_promo_sk
	AND store_sales.ss_sold_date_sk = date_dim.d_date_sk
	AND promotion.p_channel_email = 'Y'
	AND promotion.p_channel_dmail = 'Y'
	AND promotion.p_channel_tv = 'Y'
	AND date_dim.d_moy = 11
	AND date_dim.d_year = 1998
DISTRIBUTE BY ss_sold_date_sk;