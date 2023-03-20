SELECT
	item.i_brand,
	date_dim.d_month_seq,
	web_sales.ws_ship_date_sk,
	web_sales.ws_ship_mode_sk,
	item.i_brand_id,
	date_dim.d_date_sk,
	web_sales.ws_order_number,
	date_dim.d_qoy,
	web_sales.ws_promo_sk,
	web_sales.ws_ship_hdemo_sk,
	date_dim.d_date,
	web_sales.ws_web_site_sk,
	web_sales.ws_sold_time_sk,
	item.i_manager_id,
	item.i_class_id,
	item.i_item_desc,
	item.i_class,
	web_sales.ws_sales_price,
	item.i_category,
	web_sales.ws_ext_list_price,
	item.i_size,
	web_sales.ws_warehouse_sk,
	item.i_item_id,
	date_dim.d_moy,
	item.i_current_price,
	web_sales.ws_quantity,
	web_sales.ws_bill_addr_sk,
	date_dim.d_week_seq,
	web_sales.ws_web_page_sk,
	date_dim.d_year,
	web_sales.ws_net_paid,
	web_sales.ws_ext_ship_cost,
	item.i_manufact,
	item.i_units,
	web_sales.ws_ship_customer_sk,
	web_sales.ws_ext_discount_amt,
	web_sales.ws_ext_wholesale_cost,
	item.i_manufact_id,
	date_dim.d_quarter_name,
	web_sales.ws_wholesale_cost,
	item.i_item_sk,
	item.i_product_name,
	web_sales.ws_list_price,
	item.i_wholesale_cost,
	web_sales.ws_sold_date_sk,
	item.i_color,
	web_sales.ws_ext_sales_price,
	web_sales.ws_ship_addr_sk,
	date_dim.d_day_name,
	item.i_category_id,
	date_dim.d_dom,
	web_sales.ws_bill_customer_sk,
	web_sales.ws_net_profit,
	web_sales.ws_item_sk,
	date_dim.d_dow
FROM
	date_dim,
	web_sales,
	item
WHERE
	(((item.i_category = 'Sports') OR (item.i_category = 'Books') OR (item.i_category = 'Home')))
	AND web_sales.ws_item_sk = item.i_item_sk
	AND web_sales.ws_sold_date_sk = date_dim.d_date_sk
	AND (((date_dim.d_date >= cast('1970-01-01' as date) + interval 10644 days) AND (date_dim.d_date <= cast('1970-01-01' as date) + interval 10674 days)))