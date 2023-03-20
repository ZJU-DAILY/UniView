CREATE MATERIALIZED VIEW IF NOT EXISTS mv79
PARTITIONED BY (cs_sold_date_sk)
AS
SELECT
	customer_demographics.cd_purchase_estimate,
	catalog_sales.cs_ship_customer_sk,
	customer_demographics.cd_dep_college_count,
	catalog_sales.cs_ext_ship_cost,
	catalog_sales.cs_catalog_page_sk,
	customer.c_birth_month,
	customer_demographics.cd_dep_count,
	catalog_sales.cs_call_center_sk,
	catalog_sales.cs_bill_cdemo_sk,
	catalog_sales.cs_order_number,
	catalog_sales.cs_ext_sales_price,
	customer_demographics.cd_education_status,
	catalog_sales.cs_wholesale_cost,
	customer.c_current_addr_sk,
	customer.c_birth_country,
	catalog_sales.cs_bill_addr_sk,
	customer.c_first_name,
	catalog_sales.cs_net_paid,
	customer_demographics.cd_demo_sk,
	customer.c_last_review_date_sk,
	catalog_sales.cs_ext_wholesale_cost,
	catalog_sales.cs_ship_date_sk,
	customer.c_current_cdemo_sk,
	catalog_sales.cs_promo_sk,
	catalog_sales.cs_item_sk,
	catalog_sales.cs_ship_mode_sk,
	catalog_sales.cs_bill_customer_sk,
	catalog_sales.cs_ext_list_price,
	customer.c_last_name,
	customer.c_birth_day,
	customer.c_email_address,
	customer_demographics.cd_gender,
	customer.c_first_shipto_date_sk,
	catalog_sales.cs_bill_hdemo_sk,
	customer.c_customer_sk,
	customer_demographics.cd_marital_status,
	customer.c_birth_year,
	catalog_sales.cs_warehouse_sk,
	customer_demographics.cd_dep_employed_count,
	customer_demographics.cd_credit_rating,
	catalog_sales.cs_sold_time_sk,
	customer.c_customer_id,
	customer.c_login,
	catalog_sales.cs_sales_price,
	catalog_sales.cs_quantity,
	catalog_sales.cs_coupon_amt,
	customer.c_current_hdemo_sk,
	catalog_sales.cs_list_price,
	catalog_sales.cs_net_profit,
	catalog_sales.cs_ship_addr_sk,
	customer.c_first_sales_date_sk,
	customer.c_salutation,
	catalog_sales.cs_net_paid_inc_tax,
	catalog_sales.cs_ext_discount_amt,
	customer.c_preferred_cust_flag,
	catalog_sales.cs_sold_date_sk
FROM
	catalog_sales,
	customer_demographics,
	customer
WHERE
	catalog_sales.cs_bill_cdemo_sk = customer_demographics.cd_demo_sk
	AND catalog_sales.cs_bill_customer_sk = customer.c_customer_sk
	AND customer_demographics.cd_education_status = 'Unknown'
	AND customer_demographics.cd_gender = 'F'
	AND (((customer.c_birth_month = 1) OR (customer.c_birth_month = 6) OR (customer.c_birth_month = 8) OR (customer.c_birth_month = 9) OR (customer.c_birth_month = 12) OR (customer.c_birth_month = 2)))
DISTRIBUTE BY cs_sold_date_sk;