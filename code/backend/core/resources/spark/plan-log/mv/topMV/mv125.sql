CREATE MATERIALIZED VIEW IF NOT EXISTS mv125 AS
SELECT
	date_dim.d_dow,
	customer_address.ca_location_type,
	customer_address.ca_city,
	date_dim.d_quarter_name,
	web_returns.wr_item_sk,
	customer_address.ca_state,
	date_dim.d_day_name,
	web_returns.wr_order_number,
	customer_address.ca_country,
	date_dim.d_qoy,
	web_returns.wr_returned_date_sk,
	customer_address.ca_street_number,
	customer_address.ca_address_sk,
	date_dim.d_dom,
	web_returns.wr_refunded_cash,
	web_returns.wr_net_loss,
	web_returns.wr_web_page_sk,
	web_returns.wr_refunded_addr_sk,
	web_returns.wr_return_amt,
	web_returns.wr_returning_cdemo_sk,
	customer_address.ca_street_name,
	customer_address.ca_zip,
	web_returns.wr_returning_addr_sk,
	date_dim.d_week_seq,
	web_returns.wr_refunded_cdemo_sk,
	date_dim.d_year,
	customer_address.ca_county,
	web_returns.wr_fee,
	date_dim.d_month_seq,
	customer_address.ca_street_type,
	web_returns.wr_returning_customer_sk,
	customer_address.ca_gmt_offset,
	web_returns.wr_return_quantity,
	customer_address.ca_suite_number,
	date_dim.d_moy,
	date_dim.d_date,
	web_returns.wr_reason_sk,
	date_dim.d_date_sk
FROM
	web_returns,
	customer_address,
	date_dim
WHERE
	web_returns.wr_returning_addr_sk = customer_address.ca_address_sk
	AND web_returns.wr_returned_date_sk = date_dim.d_date_sk
	AND date_dim.d_year = 2002