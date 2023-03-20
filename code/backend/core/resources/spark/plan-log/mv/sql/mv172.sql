SELECT
	item.i_manufact,
	customer_address.ca_location_type,
	customer_address.ca_city,
	web_sales.ws_ship_hdemo_sk,
	date_dim.d_quarter_name,
	date_dim.d_day_name,
	customer_address.ca_state,
	customer_address.ca_country,
	item.i_manager_id,
	date_dim.d_qoy,
	item.i_class_id,
	web_sales.ws_list_price,
	customer_address.ca_street_number,
	customer_address.ca_address_sk,
	web_sales.ws_order_number,
	web_sales.ws_web_page_sk,
	customer.c_first_name,
	customer.c_first_sales_date_sk,
	customer_address.ca_street_name,
	customer.c_last_name,
	customer_address.ca_zip,
	web_sales.ws_warehouse_sk,
	date_dim.d_week_seq,
	web_sales.ws_ship_date_sk,
	date_dim.d_year,
	item.i_item_sk,
	item.i_product_name,
	web_sales.ws_net_profit,
	customer_address.ca_county,
	web_sales.ws_sold_time_sk,
	item.i_wholesale_cost,
	web_sales.ws_sold_date_sk,
	web_sales.ws_web_site_sk,
	customer.c_current_cdemo_sk,
	customer_address.ca_gmt_offset,
	customer.c_customer_sk,
	item.i_class,
	customer.c_current_hdemo_sk,
	customer.c_birth_day,
	customer.c_login,
	item.i_category,
	customer.c_salutation,
	web_sales.ws_ext_sales_price,
	web_sales.ws_ext_discount_amt,
	date_dim.d_date_sk,
	item.i_category_id,
	customer.c_first_shipto_date_sk,
	web_sales.ws_ship_mode_sk,
	customer.c_birth_year,
	customer.c_current_addr_sk,
	date_dim.d_dow,
	web_sales.ws_wholesale_cost,
	web_sales.ws_ext_list_price,
	customer.c_birth_country,
	customer.c_birth_month,
	customer.c_customer_id,
	date_dim.d_dom,
	web_sales.ws_ship_customer_sk,
	item.i_color,
	web_sales.ws_ship_addr_sk,
	web_sales.ws_ext_ship_cost,
	web_sales.ws_bill_addr_sk,
	item.i_size,
	item.i_units,
	web_sales.ws_bill_customer_sk,
	item.i_brand_id,
	item.i_brand,
	web_sales.ws_sales_price,
	item.i_current_price,
	customer_address.ca_street_type,
	date_dim.d_month_seq,
	item.i_item_id,
	web_sales.ws_ext_wholesale_cost,
	customer.c_last_review_date_sk,
	web_sales.ws_item_sk,
	item.i_manufact_id,
	web_sales.ws_net_paid,
	web_sales.ws_promo_sk,
	customer.c_email_address,
	customer_address.ca_suite_number,
	web_sales.ws_quantity,
	item.i_item_desc,
	date_dim.d_moy,
	date_dim.d_date,
	customer.c_preferred_cust_flag
FROM
	item,
	web_sales,
	customer_address,
	customer,
	date_dim
WHERE
	web_sales.ws_item_sk = item.i_item_sk
	AND web_sales.ws_bill_customer_sk = customer.c_customer_sk
	AND web_sales.ws_sold_date_sk = date_dim.d_date_sk
	AND customer.c_current_addr_sk = customer_address.ca_address_sk
	AND date_dim.d_qoy = 2
	AND date_dim.d_year = 2001