CREATE MATERIALIZED VIEW IF NOT EXISTS mv25 AS
SELECT
	store_sales.ss_ext_sales_price,
	store_sales.ss_list_price,
	store_sales.ss_promo_sk,
	store_sales.ss_ticket_number,
	store_sales.ss_ext_discount_amt,
	store_sales.ss_store_sk,
	customer_demographics.cd_education_status,
	store_sales.ss_quantity,
	store_sales.ss_ext_tax,
	store_sales.ss_item_sk,
	store_sales.ss_customer_sk,
	customer_demographics.cd_dep_employed_count,
	customer_demographics.cd_purchase_estimate,
	store_sales.ss_ext_wholesale_cost,
	customer_demographics.cd_marital_status,
	store_sales.ss_ext_list_price,
	customer_demographics.cd_dep_count,
	customer_demographics.cd_demo_sk,
	store_sales.ss_sold_date_sk,
	customer_demographics.cd_dep_college_count,
	store_sales.ss_cdemo_sk,
	store_sales.ss_coupon_amt,
	store_sales.ss_sales_price,
	store_sales.ss_sold_time_sk,
	store_sales.ss_addr_sk,
	store_sales.ss_hdemo_sk,
	customer_demographics.cd_credit_rating,
	store_sales.ss_net_profit,
	customer_demographics.cd_gender,
	store_sales.ss_wholesale_cost,
	store_sales.ss_net_paid
FROM
	store_sales,
	customer_demographics
WHERE
	customer_demographics.cd_gender = 'M'
	AND customer_demographics.cd_education_status = 'College'
	AND customer_demographics.cd_marital_status = 'S'
	AND store_sales.ss_cdemo_sk = customer_demographics.cd_demo_sk