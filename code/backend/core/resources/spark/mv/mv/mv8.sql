CREATE MATERIALIZED VIEW IF NOT EXISTS mv8
PARTITIONED BY (ss_sold_date_sk)
AS
SELECT
	date_dim.d_month_seq,
	store_sales.ss_hdemo_sk,
	store_sales.ss_ext_sales_price,
	date_dim.d_date_sk,
	customer.c_birth_month,
	date_dim.d_qoy,
	store_sales.ss_ticket_number,
	store_sales.ss_cdemo_sk,
	date_dim.d_date,
	store_sales.ss_list_price,
	store_sales.ss_store_sk,
	store_sales.ss_ext_wholesale_cost,
	customer.c_birth_country,
	customer.c_current_addr_sk,
	customer.c_first_name,
	customer.c_last_review_date_sk,
	date_dim.d_moy,
	store_sales.ss_ext_list_price,
	date_dim.d_week_seq,
	store_sales.ss_customer_sk,
	customer.c_current_cdemo_sk,
	date_dim.d_year,
	customer.c_last_name,
	store_sales.ss_net_profit,
	customer.c_birth_day,
	customer.c_email_address,
	store_sales.ss_sold_date_sk,
	customer.c_first_shipto_date_sk,
	store_sales.ss_addr_sk,
	date_dim.d_quarter_name,
	store_sales.ss_sales_price,
	store_sales.ss_item_sk,
	store_sales.ss_coupon_amt,
	customer.c_customer_sk,
	store_sales.ss_sold_time_sk,
	customer.c_birth_year,
	store_sales.ss_ext_discount_amt,
	customer.c_customer_id,
	customer.c_login,
	store_sales.ss_promo_sk,
	store_sales.ss_wholesale_cost,
	customer.c_first_sales_date_sk,
	date_dim.d_day_name,
	date_dim.d_dom,
	store_sales.ss_ext_tax,
	customer.c_salutation,
	store_sales.ss_net_paid,
	store_sales.ss_quantity,
	date_dim.d_dow,
	customer.c_preferred_cust_flag,
	customer.c_current_hdemo_sk
FROM
	date_dim,
	store_sales,
	customer
WHERE
	store_sales.ss_sold_date_sk = date_dim.d_date_sk
	AND customer.c_customer_sk = store_sales.ss_customer_sk
	AND (((date_dim.d_year = 2000) OR (date_dim.d_year = 2001) OR (date_dim.d_year = 2002) OR (date_dim.d_year = 2003)))
DISTRIBUTE BY ss_sold_date_sk;