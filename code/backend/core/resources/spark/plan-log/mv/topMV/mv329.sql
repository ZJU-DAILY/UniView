CREATE MATERIALIZED VIEW IF NOT EXISTS mv329 AS
SELECT
	date_dim.d_dow,
	customer_address.ca_location_type,
	customer_address.ca_city,
	date_dim.d_quarter_name,
	date_dim.d_day_name,
	customer_address.ca_state,
	customer_address.ca_country,
	date_dim.d_qoy,
	catalog_returns.cr_returned_date_sk,
	customer_address.ca_street_number,
	customer_address.ca_address_sk,
	date_dim.d_dom,
	catalog_returns.cr_call_center_sk,
	catalog_returns.cr_return_amt_inc_tax,
	catalog_returns.cr_order_number,
	catalog_returns.cr_reversed_charge,
	customer_address.ca_street_name,
	customer_address.ca_zip,
	catalog_returns.cr_returning_addr_sk,
	date_dim.d_week_seq,
	catalog_returns.cr_store_credit,
	date_dim.d_year,
	catalog_returns.cr_returning_customer_sk,
	customer_address.ca_county,
	catalog_returns.cr_return_amount,
	date_dim.d_month_seq,
	customer_address.ca_street_type,
	customer_address.ca_gmt_offset,
	catalog_returns.cr_item_sk,
	catalog_returns.cr_refunded_cash,
	catalog_returns.cr_net_loss,
	customer_address.ca_suite_number,
	catalog_returns.cr_return_quantity,
	date_dim.d_moy,
	date_dim.d_date,
	catalog_returns.cr_catalog_page_sk,
	date_dim.d_date_sk
FROM
	customer_address,
	catalog_returns,
	date_dim
WHERE
	catalog_returns.cr_returned_date_sk = date_dim.d_date_sk
	AND catalog_returns.cr_returning_addr_sk = customer_address.ca_address_sk
	AND date_dim.d_year = 2000