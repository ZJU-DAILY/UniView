CREATE MATERIALIZED VIEW IF NOT EXISTS mv366
PARTITIONED BY (cr_returned_date_sk)
AS
SELECT
	customer_address.ca_location_type,
	catalog_returns.cr_item_sk,
	date_dim.d_month_seq,
	customer_address.ca_street_type,
	date_dim.d_date_sk,
	customer.c_birth_month,
	date_dim.d_qoy,
	catalog_returns.cr_reversed_charge,
	customer_address.ca_county,
	customer_address.ca_city,
	date_dim.d_date,
	call_center.cc_manager,
	customer_address.ca_street_name,
	customer.c_current_addr_sk,
	customer.c_birth_country,
	catalog_returns.cr_order_number,
	call_center.cc_call_center_id,
	customer.c_first_name,
	customer.c_last_review_date_sk,
	catalog_returns.cr_returning_addr_sk,
	catalog_returns.cr_net_loss,
	customer_address.ca_zip,
	date_dim.d_moy,
	date_dim.d_week_seq,
	customer.c_current_cdemo_sk,
	call_center.cc_call_center_sk,
	date_dim.d_year,
	customer_address.ca_address_sk,
	customer.c_last_name,
	customer.c_birth_day,
	customer.c_email_address,
	catalog_returns.cr_return_quantity,
	catalog_returns.cr_store_credit,
	customer.c_first_shipto_date_sk,
	call_center.cc_name,
	date_dim.d_quarter_name,
	catalog_returns.cr_return_amount,
	catalog_returns.cr_return_amt_inc_tax,
	customer_address.ca_street_number,
	customer.c_customer_sk,
	catalog_returns.cr_call_center_sk,
	customer_address.ca_state,
	customer.c_birth_year,
	customer_address.ca_gmt_offset,
	catalog_returns.cr_refunded_cash,
	customer.c_customer_id,
	customer.c_login,
	catalog_returns.cr_returning_customer_sk,
	customer_address.ca_country,
	catalog_returns.cr_returned_date_sk,
	customer_address.ca_suite_number,
	catalog_returns.cr_catalog_page_sk,
	call_center.cc_county,
	customer.c_first_sales_date_sk,
	date_dim.d_day_name,
	date_dim.d_dom,
	customer.c_salutation,
	date_dim.d_dow,
	customer.c_preferred_cust_flag,
	customer.c_current_hdemo_sk
FROM
	date_dim,
	customer_address,
	customer,
	call_center,
	catalog_returns
WHERE
	catalog_returns.cr_returned_date_sk = date_dim.d_date_sk
	AND call_center.cc_call_center_sk = catalog_returns.cr_call_center_sk
	AND catalog_returns.cr_returning_customer_sk = customer.c_customer_sk
	AND date_dim.d_year = 1998
	AND date_dim.d_moy = 11
	AND customer.c_current_addr_sk = customer_address.ca_address_sk
DISTRIBUTE BY cr_returned_date_sk;