CREATE MATERIALIZED VIEW IF NOT EXISTS mv368
PARTITIONED BY (cr_returned_date_sk)
AS
SELECT
	customer_demographics.cd_purchase_estimate,
	catalog_returns.cr_item_sk,
	household_demographics.hd_vehicle_count,
	customer.c_birth_month,
	date_dim.d_qoy,
	customer_demographics.cd_dep_count,
	catalog_returns.cr_reversed_charge,
	customer_address.ca_city,
	call_center.cc_manager,
	customer_address.ca_street_name,
	customer.c_birth_country,
	catalog_returns.cr_order_number,
	call_center.cc_call_center_id,
	customer.c_last_review_date_sk,
	customer_demographics.cd_demo_sk,
	date_dim.d_moy,
	date_dim.d_week_seq,
	household_demographics.hd_dep_count,
	customer.c_current_cdemo_sk,
	customer.c_last_name,
	customer.c_birth_day,
	customer.c_email_address,
	catalog_returns.cr_store_credit,
	household_demographics.hd_demo_sk,
	customer_demographics.cd_gender,
	date_dim.d_quarter_name,
	customer_address.ca_street_number,
	catalog_returns.cr_call_center_sk,
	customer_address.ca_state,
	customer.c_birth_year,
	customer_demographics.cd_dep_employed_count,
	catalog_returns.cr_refunded_cash,
	household_demographics.hd_income_band_sk,
	catalog_returns.cr_returning_customer_sk,
	customer_address.ca_country,
	catalog_returns.cr_returned_date_sk,
	customer_address.ca_suite_number,
	catalog_returns.cr_catalog_page_sk,
	date_dim.d_dom,
	customer.c_preferred_cust_flag,
	customer_address.ca_location_type,
	date_dim.d_month_seq,
	customer_address.ca_street_type,
	customer_demographics.cd_dep_college_count,
	date_dim.d_date_sk,
	household_demographics.hd_buy_potential,
	customer_address.ca_county,
	date_dim.d_date,
	customer_demographics.cd_education_status,
	customer.c_current_addr_sk,
	customer.c_first_name,
	catalog_returns.cr_returning_addr_sk,
	catalog_returns.cr_net_loss,
	customer_address.ca_zip,
	call_center.cc_call_center_sk,
	date_dim.d_year,
	customer_address.ca_address_sk,
	catalog_returns.cr_return_quantity,
	customer.c_first_shipto_date_sk,
	call_center.cc_name,
	catalog_returns.cr_return_amount,
	catalog_returns.cr_return_amt_inc_tax,
	customer.c_customer_sk,
	customer_demographics.cd_marital_status,
	customer_demographics.cd_credit_rating,
	customer_address.ca_gmt_offset,
	customer.c_customer_id,
	customer.c_login,
	call_center.cc_county,
	customer.c_first_sales_date_sk,
	date_dim.d_day_name,
	customer.c_salutation,
	date_dim.d_dow,
	customer.c_current_hdemo_sk
FROM
	date_dim,
	customer_address,
	customer,
	call_center,
	catalog_returns,
	customer_demographics,
	household_demographics
WHERE
	customer.c_current_hdemo_sk = household_demographics.hd_demo_sk
	AND customer.c_current_addr_sk = customer_address.ca_address_sk
	AND catalog_returns.cr_returning_customer_sk = customer.c_customer_sk
	AND customer.c_current_cdemo_sk = customer_demographics.cd_demo_sk
	AND (((customer_demographics.cd_education_status = 'Unknown') OR (customer_demographics.cd_education_status = 'Advanced Degree')))
	AND (((customer_demographics.cd_marital_status = 'M') OR (customer_demographics.cd_marital_status = 'W')))
	AND date_dim.d_moy = 11
	AND catalog_returns.cr_returned_date_sk = date_dim.d_date_sk
	AND date_dim.d_year = 1998
	AND call_center.cc_call_center_sk = catalog_returns.cr_call_center_sk
DISTRIBUTE BY cr_returned_date_sk;