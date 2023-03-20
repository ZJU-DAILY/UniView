CREATE MATERIALIZED VIEW IF NOT EXISTS mv125
PARTITIONED BY (wr_returned_date_sk)
AS
SELECT
	web_returns.wr_returning_customer_sk,
	customer_address.ca_location_type,
	date_dim.d_month_seq,
	customer_address.ca_street_type,
	web_returns.wr_item_sk,
	date_dim.d_date_sk,
	date_dim.d_qoy,
	web_returns.wr_refunded_cdemo_sk,
	customer_address.ca_county,
	customer_address.ca_city,
	web_returns.wr_refunded_addr_sk,
	date_dim.d_date,
	customer_address.ca_street_name,
	customer_address.ca_zip,
	date_dim.d_moy,
	web_returns.wr_return_quantity,
	web_returns.wr_returning_addr_sk,
	date_dim.d_week_seq,
	date_dim.d_year,
	customer_address.ca_address_sk,
	web_returns.wr_fee,
	web_returns.wr_refunded_cash,
	web_returns.wr_return_amt,
	date_dim.d_quarter_name,
	web_returns.wr_net_loss,
	web_returns.wr_web_page_sk,
	customer_address.ca_street_number,
	web_returns.wr_reason_sk,
	customer_address.ca_state,
	customer_address.ca_gmt_offset,
	web_returns.wr_returned_date_sk,
	customer_address.ca_country,
	customer_address.ca_suite_number,
	web_returns.wr_order_number,
	web_returns.wr_returning_cdemo_sk,
	date_dim.d_day_name,
	date_dim.d_dom,
	date_dim.d_dow
FROM
	date_dim,
	customer_address,
	web_returns
WHERE
	web_returns.wr_returned_date_sk = date_dim.d_date_sk
	AND web_returns.wr_returning_addr_sk = customer_address.ca_address_sk
	AND date_dim.d_year = 2002
DISTRIBUTE BY wr_returned_date_sk;