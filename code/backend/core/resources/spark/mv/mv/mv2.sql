CREATE MATERIALIZED VIEW IF NOT EXISTS mv2
PARTITIONED BY (sr_returned_date_sk)
AS
SELECT
	date_dim.d_month_seq,
	store.s_suite_number,
	date_dim.d_date_sk,
	customer.c_birth_month,
	date_dim.d_qoy,
	store.s_store_name,
	store.s_zip,
	date_dim.d_date,
	store_returns.sr_ticket_number,
	store.s_store_id,
	customer.c_birth_country,
	customer.c_current_addr_sk,
	customer.c_first_name,
	store.s_company_name,
	store.s_street_name,
	store.s_city,
	customer.c_last_review_date_sk,
	store_returns.sr_reason_sk,
	store.s_market_id,
	date_dim.d_moy,
	date_dim.d_week_seq,
	store.s_gmt_offset,
	store_returns.sr_return_quantity,
	customer.c_current_cdemo_sk,
	date_dim.d_year,
	store_returns.sr_cdemo_sk,
	customer.c_last_name,
	store.s_state,
	customer.c_birth_day,
	customer.c_email_address,
	store_returns.sr_customer_sk,
	customer.c_first_shipto_date_sk,
	date_dim.d_quarter_name,
	store.s_company_id,
	customer.c_customer_sk,
	customer.c_birth_year,
	store_returns.sr_net_loss,
	store.s_store_sk,
	store_returns.sr_returned_date_sk,
	customer.c_customer_id,
	customer.c_login,
	store_returns.sr_item_sk,
	store.s_number_employees,
	customer.c_first_sales_date_sk,
	store_returns.sr_return_amt,
	date_dim.d_day_name,
	store.s_street_number,
	date_dim.d_dom,
	store.s_county,
	customer.c_salutation,
	store_returns.sr_store_sk,
	store.s_street_type,
	date_dim.d_dow,
	customer.c_preferred_cust_flag,
	customer.c_current_hdemo_sk
FROM
	date_dim,
	store_returns,
	store,
	customer
WHERE
	store.s_state = 'TN'
	AND store_returns.sr_store_sk = store.s_store_sk
	AND store_returns.sr_customer_sk = customer.c_customer_sk
	AND store_returns.sr_returned_date_sk = date_dim.d_date_sk
	AND date_dim.d_year = 2000
DISTRIBUTE BY sr_returned_date_sk;