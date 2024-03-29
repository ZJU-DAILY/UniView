SELECT
	web_sales.ws_ship_mode_sk,
	web_sales.ws_order_number,
	customer.c_birth_month,
	date_dim.d_qoy,
	web_sales.ws_ship_hdemo_sk,
	web_sales.ws_web_site_sk,
	web_sales.ws_sold_time_sk,
	customer.c_birth_country,
	web_sales.ws_sales_price,
	customer.c_last_review_date_sk,
	date_dim.d_moy,
	web_sales.ws_bill_addr_sk,
	date_dim.d_week_seq,
	customer.c_current_cdemo_sk,
	web_sales.ws_ext_ship_cost,
	customer.c_last_name,
	customer.c_birth_day,
	customer.c_email_address,
	web_sales.ws_ship_customer_sk,
	date_dim.d_quarter_name,
	web_sales.ws_wholesale_cost,
	customer.c_birth_year,
	web_sales.ws_ext_sales_price,
	date_dim.d_dom,
	customer.c_preferred_cust_flag,
	date_dim.d_month_seq,
	web_sales.ws_ship_date_sk,
	date_dim.d_date_sk,
	web_sales.ws_promo_sk,
	date_dim.d_date,
	customer.c_current_addr_sk,
	web_sales.ws_ext_list_price,
	customer.c_first_name,
	customer.c_salutation,
	web_sales.ws_warehouse_sk,
	web_sales.ws_quantity,
	web_sales.ws_web_page_sk,
	date_dim.d_year,
	web_sales.ws_net_paid,
	web_sales.ws_ext_discount_amt,
	customer.c_first_shipto_date_sk,
	web_sales.ws_ext_wholesale_cost,
	customer.c_customer_sk,
	web_sales.ws_list_price,
	web_sales.ws_sold_date_sk,
	customer.c_customer_id,
	customer.c_login,
	web_sales.ws_ship_addr_sk,
	customer.c_first_sales_date_sk,
	date_dim.d_day_name,
	web_sales.ws_bill_customer_sk,
	web_sales.ws_net_profit,
	web_sales.ws_item_sk,
	date_dim.d_dow,
	customer.c_current_hdemo_sk
FROM
	customer SEMI JOIN store_sales ON (customer.c_customer_sk = store_sales.ss_customer_sk)	Join date_dim
	Join web_sales
WHERE
	date_dim.d_moy <= 4
	AND date_dim.d_moy >= 1
	AND date_dim.d_year = 2002
	AND store_sales.ss_sold_date_sk = date_dim.d_date_sk
	AND web_sales.ws_sold_date_sk = date_dim.d_date_sk
	AND customer.c_customer_sk = web_sales.ws_bill_customer_sk