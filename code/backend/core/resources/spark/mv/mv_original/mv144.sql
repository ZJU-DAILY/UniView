SELECT
	date_dim.d_month_seq,
	date_dim.d_date_sk,
	customer.c_birth_month,
	date_dim.d_qoy,
	date_dim.d_date,
	customer.c_current_addr_sk,
	customer.c_birth_country,
	customer.c_first_name,
	customer.c_last_review_date_sk,
	date_dim.d_moy,
	date_dim.d_week_seq,
	customer.c_current_cdemo_sk,
	date_dim.d_year,
	customer.c_last_name,
	customer.c_birth_day,
	customer.c_email_address,
	customer.c_first_shipto_date_sk,
	date_dim.d_quarter_name,
	customer.c_customer_sk,
	customer.c_birth_year,
	customer.c_customer_id,
	customer.c_login,
	customer.c_first_sales_date_sk,
	date_dim.d_day_name,
	date_dim.d_dom,
	customer.c_salutation,
	date_dim.d_dow,
	customer.c_preferred_cust_flag,
	customer.c_current_hdemo_sk
FROM
	customer SEMI JOIN store_sales ON (customer.c_customer_sk = store_sales.ss_customer_sk)	Join date_dim
WHERE
	store_sales.ss_sold_date_sk = date_dim.d_date_sk
	AND date_dim.d_qoy < 4
	AND date_dim.d_year = 2002