CREATE MATERIALIZED VIEW IF NOT EXISTS mv330
PARTITIONED BY (cr_returned_date_sk)
AS
SELECT
	customer_address.ca_location_type,
	catalog_returns.cr_item_sk,
	date_dim.d_month_seq,
	customer_address.ca_street_type,
	date_dim.d_date_sk,
	date_dim.d_qoy,
	catalog_returns.cr_reversed_charge,
	customer_address.ca_county,
	customer_address.ca_city,
	date_dim.d_date,
	customer_address.ca_street_name,
	catalog_returns.cr_order_number,
	catalog_returns.cr_returning_addr_sk,
	catalog_returns.cr_net_loss,
	customer_address.ca_zip,
	date_dim.d_moy,
	date_dim.d_week_seq,
	date_dim.d_year,
	customer_address.ca_address_sk,
	catalog_returns.cr_return_quantity,
	catalog_returns.cr_store_credit,
	date_dim.d_quarter_name,
	catalog_returns.cr_return_amount,
	catalog_returns.cr_return_amt_inc_tax,
	customer_address.ca_street_number,
	catalog_returns.cr_call_center_sk,
	customer_address.ca_state,
	customer_address.ca_gmt_offset,
	catalog_returns.cr_refunded_cash,
	catalog_returns.cr_returning_customer_sk,
	customer_address.ca_country,
	catalog_returns.cr_returned_date_sk,
	catalog_returns.cr_catalog_page_sk,
	customer_address.ca_suite_number,
	date_dim.d_day_name,
	date_dim.d_dom,
	date_dim.d_dow
FROM
	date_dim,
	catalog_returns,
	customer_address
WHERE
	catalog_returns.cr_returned_date_sk = date_dim.d_date_sk
	AND catalog_returns.cr_returning_addr_sk = customer_address.ca_address_sk
	AND date_dim.d_year = 2000
DISTRIBUTE BY cr_returned_date_sk;