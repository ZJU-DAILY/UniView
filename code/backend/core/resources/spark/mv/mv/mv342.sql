CREATE MATERIALIZED VIEW IF NOT EXISTS mv342
AS
SELECT
	customer_address.ca_location_type,
	customer.c_last_name,
	customer.c_birth_day,
	customer.c_email_address,
	customer_address.ca_street_type,
	customer.c_first_shipto_date_sk,
	customer.c_birth_month,
	customer_address.ca_street_number,
	customer.c_customer_sk,
	customer_address.ca_county,
	customer_address.ca_state,
	customer_address.ca_city,
	customer.c_birth_year,
	customer_address.ca_gmt_offset,
	customer.c_customer_id,
	customer_address.ca_street_name,
	customer.c_login,
	customer_address.ca_country,
	customer.c_current_addr_sk,
	customer.c_birth_country,
	customer_address.ca_suite_number,
	customer.c_first_name,
	customer.c_last_review_date_sk,
	customer.c_first_sales_date_sk,
	customer_address.ca_zip,
	customer_address.ca_address_sk,
	customer.c_salutation,
	customer.c_current_cdemo_sk,
	customer.c_preferred_cust_flag,
	customer.c_current_hdemo_sk
FROM
	customer_address,
	customer
WHERE
	customer.c_current_addr_sk = customer_address.ca_address_sk
	AND customer_address.ca_city = 'Edgewood';