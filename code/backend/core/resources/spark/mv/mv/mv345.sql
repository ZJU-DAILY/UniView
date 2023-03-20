CREATE MATERIALIZED VIEW IF NOT EXISTS mv345
AS
SELECT
	customer_address.ca_location_type,
	customer_demographics.cd_purchase_estimate,
	customer_demographics.cd_dep_college_count,
	income_band.ib_upper_bound,
	customer_address.ca_street_type,
	household_demographics.hd_vehicle_count,
	customer.c_birth_month,
	customer_demographics.cd_dep_count,
	household_demographics.hd_buy_potential,
	customer_address.ca_county,
	customer_address.ca_city,
	income_band.ib_lower_bound,
	customer_address.ca_street_name,
	customer_demographics.cd_education_status,
	customer.c_current_addr_sk,
	customer.c_birth_country,
	customer.c_first_name,
	customer.c_last_review_date_sk,
	customer_demographics.cd_demo_sk,
	customer_address.ca_zip,
	household_demographics.hd_dep_count,
	customer.c_current_cdemo_sk,
	customer_address.ca_address_sk,
	customer.c_last_name,
	customer.c_birth_day,
	customer.c_email_address,
	customer_demographics.cd_gender,
	household_demographics.hd_demo_sk,
	customer.c_first_shipto_date_sk,
	customer_address.ca_street_number,
	income_band.ib_income_band_sk,
	customer.c_customer_sk,
	customer_demographics.cd_marital_status,
	customer.c_birth_year,
	customer_address.ca_state,
	customer_demographics.cd_dep_employed_count,
	customer_demographics.cd_credit_rating,
	customer_address.ca_gmt_offset,
	household_demographics.hd_income_band_sk,
	customer.c_customer_id,
	customer.c_login,
	customer_address.ca_country,
	customer_address.ca_suite_number,
	customer.c_first_sales_date_sk,
	customer.c_salutation,
	customer.c_preferred_cust_flag,
	customer.c_current_hdemo_sk
FROM
	customer,
	income_band,
	customer_address,
	customer_demographics,
	household_demographics
WHERE
	(((income_band.ib_lower_bound >= 38128) AND (income_band.ib_upper_bound <= 88128)))
	AND household_demographics.hd_income_band_sk = income_band.ib_income_band_sk
	AND customer_address.ca_city = 'Edgewood'
	AND customer.c_current_addr_sk = customer_address.ca_address_sk
	AND customer.c_current_hdemo_sk = household_demographics.hd_demo_sk
	AND customer.c_current_cdemo_sk = customer_demographics.cd_demo_sk;