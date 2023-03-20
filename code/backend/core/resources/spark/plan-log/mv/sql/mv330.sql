SELECT
	customer.c_first_shipto_date_sk,
	date_dim.d_dow,
	customer.c_birth_year,
	customer_address.ca_location_type,
	customer.c_current_addr_sk,
	customer_address.ca_city,
	date_dim.d_quarter_name,
	date_dim.d_day_name,
	customer_address.ca_state,
	customer.c_birth_country,
	customer_address.ca_country,
	date_dim.d_qoy,
	catalog_returns.cr_returned_date_sk,
	customer.c_birth_month,
	customer_address.ca_street_number,
	customer.c_customer_id,
	customer_address.ca_address_sk,
	date_dim.d_dom,
	catalog_returns.cr_call_center_sk,
	catalog_returns.cr_return_amt_inc_tax,
	catalog_returns.cr_order_number,
	customer.c_first_name,
	customer.c_first_sales_date_sk,
	customer.c_last_name,
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
	customer.c_last_review_date_sk,
	customer.c_current_cdemo_sk,
	customer_address.ca_gmt_offset,
	catalog_returns.cr_item_sk,
	customer.c_email_address,
	customer.c_customer_sk,
	catalog_returns.cr_refunded_cash,
	date_dim.d_date_sk,
	catalog_returns.cr_net_loss,
	customer_address.ca_suite_number,
	catalog_returns.cr_return_quantity,
	date_dim.d_moy,
	customer.c_current_hdemo_sk,
	customer.c_birth_day,
	date_dim.d_date,
	customer.c_salutation,
	customer.c_login,
	catalog_returns.cr_catalog_page_sk,
	customer.c_preferred_cust_flag,
	catalog_returns.cr_reversed_charge
FROM
	customer_address,
	customer,
	catalog_returns,
	date_dim
WHERE
	catalog_returns.cr_returned_date_sk = date_dim.d_date_sk
	AND catalog_returns.cr_returning_addr_sk = customer_address.ca_address_sk
	AND catalog_returns.cr_returning_customer_sk = customer.c_customer_sk
	AND date_dim.d_year = 2000