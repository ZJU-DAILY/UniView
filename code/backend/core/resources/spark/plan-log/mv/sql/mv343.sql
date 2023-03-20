SELECT
	customer.c_first_shipto_date_sk,
	customer.c_birth_year,
	customer_address.ca_location_type,
	customer.c_current_addr_sk,
	customer_address.ca_city,
	customer_address.ca_state,
	customer.c_birth_country,
	customer_demographics.cd_education_status,
	customer_address.ca_country,
	customer_demographics.cd_dep_employed_count,
	customer.c_birth_month,
	customer_address.ca_street_number,
	customer.c_customer_id,
	customer_address.ca_address_sk,
	customer_demographics.cd_dep_college_count,
	customer_demographics.cd_demo_sk,
	customer.c_first_name,
	customer.c_first_sales_date_sk,
	household_demographics.hd_vehicle_count,
	customer.c_last_name,
	customer_address.ca_street_name,
	customer_address.ca_zip,
	customer_demographics.cd_credit_rating,
	household_demographics.hd_dep_count,
	household_demographics.hd_income_band_sk,
	household_demographics.hd_demo_sk,
	customer_address.ca_county,
	customer_address.ca_street_type,
	customer.c_last_review_date_sk,
	customer_demographics.cd_purchase_estimate,
	customer.c_current_cdemo_sk,
	customer_address.ca_gmt_offset,
	customer_demographics.cd_marital_status,
	household_demographics.hd_buy_potential,
	customer.c_email_address,
	customer.c_customer_sk,
	customer_demographics.cd_dep_count,
	customer_address.ca_suite_number,
	customer.c_preferred_cust_flag,
	customer.c_current_hdemo_sk,
	customer.c_birth_day,
	customer.c_login,
	customer.c_salutation,
	customer_demographics.cd_gender
FROM
	customer_address,
	customer,
	customer_demographics,
	household_demographics
WHERE
	customer.c_current_cdemo_sk = customer_demographics.cd_demo_sk
	AND customer.c_current_hdemo_sk = household_demographics.hd_demo_sk
	AND customer.c_current_addr_sk = customer_address.ca_address_sk
	AND customer_address.ca_city = 'Edgewood'