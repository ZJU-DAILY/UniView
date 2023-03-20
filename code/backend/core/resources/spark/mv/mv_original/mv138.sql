SELECT
	item.i_brand,
	customer_address.ca_location_type,
	date_dim.d_month_seq,
	web_sales.ws_ship_date_sk,
	web_sales.ws_ship_mode_sk,
	customer_address.ca_street_type,
	item.i_brand_id,
	date_dim.d_date_sk,
	web_sales.ws_order_number,
	date_dim.d_qoy,
	web_sales.ws_promo_sk,
	customer_address.ca_county,
	web_sales.ws_ship_hdemo_sk,
	web_sales.ws_bill_customer_sk,
	customer_address.ca_city,
	date_dim.d_date,
	web_sales.ws_web_site_sk,
	web_sales.ws_sold_time_sk,
	item.i_manager_id,
	item.i_class_id,
	item.i_item_desc,
	customer_address.ca_street_name,
	item.i_class,
	web_sales.ws_sales_price,
	item.i_category,
	web_sales.ws_ext_list_price,
	item.i_size,
	customer_address.ca_zip,
	web_sales.ws_warehouse_sk,
	web_sales.ws_quantity,
	date_dim.d_moy,
	web_sales.ws_bill_addr_sk,
	item.i_item_id,
	web_sales.ws_web_page_sk,
	date_dim.d_week_seq,
	item.i_current_price,
	date_dim.d_year,
	customer_address.ca_address_sk,
	web_sales.ws_net_paid,
	web_sales.ws_ext_ship_cost,
	item.i_manufact,
	item.i_units,
	web_sales.ws_ship_customer_sk,
	web_sales.ws_ext_discount_amt,
	web_sales.ws_ext_wholesale_cost,
	date_dim.d_quarter_name,
	web_sales.ws_wholesale_cost,
	customer_address.ca_street_number,
	item.i_item_sk,
	item.i_product_name,
	customer_address.ca_state,
	web_sales.ws_list_price,
	item.i_wholesale_cost,
	web_sales.ws_sold_date_sk,
	customer_address.ca_gmt_offset,
	item.i_color,
	web_sales.ws_ext_sales_price,
	web_sales.ws_ship_addr_sk,
	customer_address.ca_country,
	customer_address.ca_suite_number,
	date_dim.d_day_name,
	item.i_category_id,
	date_dim.d_dom,
	item.i_manufact_id,
	web_sales.ws_net_profit,
	web_sales.ws_item_sk,
	date_dim.d_dow
FROM
	date_dim,
	web_sales,
	item,
	customer_address
WHERE
	web_sales.ws_item_sk = item.i_item_sk
	AND web_sales.ws_bill_addr_sk = customer_address.ca_address_sk
	AND web_sales.ws_sold_date_sk = date_dim.d_date_sk
	AND item.i_category = 'Electronics'
	AND date_dim.d_year = 1998
	AND date_dim.d_moy = 5