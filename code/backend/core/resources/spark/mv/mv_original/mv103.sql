SELECT
	web_sales.ws_ship_mode_sk,
	store_sales.ss_hdemo_sk,
	item.i_brand_id,
	web_sales.ws_order_number,
	customer.c_birth_month,
	date_dim.d_qoy,
	web_sales.ws_ship_hdemo_sk,
	web_sales.ws_bill_customer_sk,
	web_sales.ws_web_site_sk,
	store_sales.ss_list_price,
	web_sales.ws_sold_time_sk,
	item.i_manager_id,
	store_sales.ss_store_sk,
	item.i_class,
	store_sales.ss_ext_wholesale_cost,
	customer.c_birth_country,
	web_sales.ws_sales_price,
	customer.c_last_review_date_sk,
	item.i_item_id,
	web_sales.ws_bill_addr_sk,
	date_dim.d_moy,
	item.i_current_price,
	store_sales.ss_customer_sk,
	date_dim.d_week_seq,
	customer.c_current_cdemo_sk,
	web_sales.ws_ext_ship_cost,
	customer.c_last_name,
	item.i_manufact,
	item.i_units,
	customer.c_birth_day,
	customer.c_email_address,
	web_sales.ws_ship_customer_sk,
	date_dim.d_quarter_name,
	web_sales.ws_wholesale_cost,
	store_sales.ss_sales_price,
	item.i_product_name,
	customer.c_birth_year,
	store_sales.ss_ext_discount_amt,
	store_sales.ss_promo_sk,
	item.i_color,
	store_sales.ss_wholesale_cost,
	web_sales.ws_ext_sales_price,
	date_dim.d_dom,
	item.i_manufact_id,
	store_sales.ss_net_paid,
	store_sales.ss_quantity,
	customer.c_preferred_cust_flag,
	item.i_brand,
	date_dim.d_month_seq,
	web_sales.ws_ship_date_sk,
	store_sales.ss_ext_sales_price,
	date_dim.d_date_sk,
	web_sales.ws_promo_sk,
	store_sales.ss_ticket_number,
	store_sales.ss_cdemo_sk,
	date_dim.d_date,
	item.i_class_id,
	item.i_item_desc,
	customer.c_current_addr_sk,
	item.i_category,
	web_sales.ws_ext_list_price,
	customer.c_first_name,
	item.i_size,
	web_sales.ws_warehouse_sk,
	web_sales.ws_quantity,
	web_sales.ws_web_page_sk,
	store_sales.ss_ext_list_price,
	date_dim.d_year,
	web_sales.ws_net_paid,
	store_sales.ss_net_profit,
	store_sales.ss_sold_date_sk,
	web_sales.ws_ext_discount_amt,
	customer.c_first_shipto_date_sk,
	store_sales.ss_addr_sk,
	web_sales.ws_ext_wholesale_cost,
	store_sales.ss_item_sk,
	store_sales.ss_coupon_amt,
	customer.c_customer_sk,
	item.i_item_sk,
	store_sales.ss_sold_time_sk,
	web_sales.ws_list_price,
	item.i_wholesale_cost,
	web_sales.ws_sold_date_sk,
	customer.c_customer_id,
	customer.c_login,
	web_sales.ws_ship_addr_sk,
	customer.c_first_sales_date_sk,
	date_dim.d_day_name,
	item.i_category_id,
	store_sales.ss_ext_tax,
	customer.c_salutation,
	web_sales.ws_net_profit,
	web_sales.ws_item_sk,
	date_dim.d_dow,
	customer.c_current_hdemo_sk
FROM
	date_dim,
	store_sales,
	web_sales,
	customer,
	item
WHERE
	web_sales.ws_item_sk = item.i_item_sk
	AND web_sales.ws_bill_customer_sk = customer.c_customer_sk
	AND date_dim.d_moy = 2
	AND (((date_dim.d_year = 2000) OR (date_dim.d_year = 2001) OR (date_dim.d_year = 2002) OR (date_dim.d_year = 2003)))
	AND date_dim.d_year = 2000
	AND store_sales.ss_sold_date_sk = date_dim.d_date_sk
	AND web_sales.ws_sold_date_sk = date_dim.d_date_sk
	AND store_sales.ss_item_sk = item.i_item_sk
	AND store_sales.ss_customer_sk = customer.c_customer_sk