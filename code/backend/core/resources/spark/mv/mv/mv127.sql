CREATE MATERIALIZED VIEW IF NOT EXISTS mv127
PARTITIONED BY (wr_returned_date_sk)
AS
SELECT
	web_returns.wr_returning_customer_sk,
	customer_address.ca_location_type,
	date_dim.d_month_seq,
	web_returns.wr_item_sk,
	customer_address.ca_street_type,
	date_dim.d_date_sk,
	customer.c_birth_month,
	date_dim.d_qoy,
	web_returns.wr_refunded_cdemo_sk,
	customer_address.ca_county,
	web_returns.wr_refunded_addr_sk,
	customer_address.ca_city,
	date_dim.d_date,
	customer_address.ca_street_name,
	customer.c_current_addr_sk,
	customer.c_birth_country,
	customer.c_first_name,
	customer.c_last_review_date_sk,
	customer_address.ca_zip,
	date_dim.d_moy,
	web_returns.wr_return_quantity,
	web_returns.wr_returning_addr_sk,
	date_dim.d_week_seq,
	customer.c_current_cdemo_sk,
	date_dim.d_year,
	customer_address.ca_address_sk,
	web_returns.wr_fee,
	customer.c_last_name,
	web_returns.wr_refunded_cash,
	customer.c_birth_day,
	customer.c_email_address,
	web_returns.wr_return_amt,
	customer.c_first_shipto_date_sk,
	date_dim.d_quarter_name,
	web_returns.wr_net_loss,
	web_returns.wr_web_page_sk,
	customer_address.ca_street_number,
	customer.c_customer_sk,
	web_returns.wr_reason_sk,
	customer_address.ca_state,
	customer.c_birth_year,
	customer_address.ca_gmt_offset,
	customer.c_customer_id,
	customer.c_login,
	web_returns.wr_returned_date_sk,
	customer_address.ca_country,
	web_returns.wr_returning_cdemo_sk,
	web_returns.wr_order_number,
	customer_address.ca_suite_number,
	customer.c_first_sales_date_sk,
	date_dim.d_day_name,
	date_dim.d_dom,
	customer.c_salutation,
	date_dim.d_dow,
	customer.c_preferred_cust_flag,
	customer.c_current_hdemo_sk
FROM
	date_dim,
	web_returns,
	customer,
	customer_address
WHERE
	date_dim.d_year = 2002
	AND web_returns.wr_returned_date_sk = date_dim.d_date_sk
	AND customer_address.ca_state = 'GA'
	AND customer.c_current_addr_sk = customer_address.ca_address_sk
	AND web_returns.wr_returning_addr_sk = customer_address.ca_address_sk
	AND web_returns.wr_returning_customer_sk = customer.c_customer_sk
DISTRIBUTE BY wr_returned_date_sk;