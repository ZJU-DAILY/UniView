SELECT
	date_dim.d_dow,
	store_sales.ss_promo_sk,
	date_dim.d_quarter_name,
	store_sales.ss_ext_discount_amt,
	date_dim.d_day_name,
	store_sales.ss_store_sk,
	customer_demographics.cd_education_status,
	store_sales.ss_item_sk,
	store_sales.ss_customer_sk,
	customer_demographics.cd_dep_employed_count,
	date_dim.d_qoy,
	date_dim.d_dom,
	customer_demographics.cd_dep_college_count,
	customer_demographics.cd_demo_sk,
	store_sales.ss_sold_date_sk,
	store_sales.ss_cdemo_sk,
	store_sales.ss_coupon_amt,
	store_sales.ss_addr_sk,
	customer_demographics.cd_credit_rating,
	date_dim.d_week_seq,
	date_dim.d_year,
	store_sales.ss_ext_sales_price,
	store_sales.ss_ticket_number,
	store_sales.ss_list_price,
	date_dim.d_month_seq,
	store_sales.ss_quantity,
	store_sales.ss_ext_tax,
	store_sales.ss_wholesale_cost,
	customer_demographics.cd_purchase_estimate,
	store_sales.ss_ext_wholesale_cost,
	customer_demographics.cd_marital_status,
	store_sales.ss_ext_list_price,
	customer_demographics.cd_dep_count,
	store_sales.ss_sales_price,
	store_sales.ss_sold_time_sk,
	date_dim.d_moy,
	date_dim.d_date,
	store_sales.ss_net_profit,
	customer_demographics.cd_gender,
	store_sales.ss_hdemo_sk,
	date_dim.d_date_sk,
	store_sales.ss_net_paid
FROM
	store_sales,
	customer_demographics,
	date_dim
WHERE
	customer_demographics.cd_gender = 'M'
	AND customer_demographics.cd_education_status = 'College'
	AND customer_demographics.cd_marital_status = 'S'
	AND store_sales.ss_cdemo_sk = customer_demographics.cd_demo_sk
	AND (date_dim.d_year = 2000 OR date_dim.d_year = 2002)
	AND store_sales.ss_sold_date_sk = date_dim.d_date_sk