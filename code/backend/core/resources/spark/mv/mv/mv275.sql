CREATE MATERIALIZED VIEW IF NOT EXISTS mv275
AS
SELECT
	web_sales.ws_ship_mode_sk,
	store_sales.ss_hdemo_sk,
	item.i_brand_id,
	web_sales.ws_order_number,
	date_dim.d_qoy,
	web_sales.ws_ship_hdemo_sk,
	web_sales.ws_web_site_sk,
	store_sales.ss_list_price,
	web_sales.ws_sold_time_sk,
	item.i_manager_id,
	store_sales.ss_store_sk,
	item.i_class,
	store_sales.ss_ext_wholesale_cost,
	web_sales.ws_sales_price,
	item.i_item_id,
	date_dim.d_moy,
	web_sales.ws_bill_addr_sk,
	item.i_current_price,
	store_sales.ss_customer_sk,
	date_dim.d_week_seq,
	web_sales.ws_ext_ship_cost,
	item.i_manufact,
	item.i_units,
	web_sales.ws_ship_customer_sk,
	date_dim.d_quarter_name,
	web_sales.ws_wholesale_cost,
	store_sales.ss_sales_price,
	item.i_product_name,
	time_dim.t_meal_time,
	store_sales.ss_ext_discount_amt,
	store_sales.ss_promo_sk,
	item.i_color,
	web_sales.ws_ext_sales_price,
	time_dim.t_minute,
	store_sales.ss_wholesale_cost,
	date_dim.d_dom,
	item.i_manufact_id,
	store_sales.ss_net_paid,
	store_sales.ss_quantity,
	item.i_brand,
	date_dim.d_month_seq,
	web_sales.ws_ship_date_sk,
	store_sales.ss_ext_sales_price,
	date_dim.d_date_sk,
	web_sales.ws_promo_sk,
	time_dim.t_time_sk,
	store_sales.ss_ticket_number,
	store_sales.ss_cdemo_sk,
	date_dim.d_date,
	item.i_class_id,
	item.i_item_desc,
	item.i_category,
	web_sales.ws_ext_list_price,
	item.i_size,
	web_sales.ws_warehouse_sk,
	web_sales.ws_quantity,
	store_sales.ss_ext_list_price,
	web_sales.ws_web_page_sk,
	date_dim.d_year,
	web_sales.ws_net_paid,
	store_sales.ss_net_profit,
	store_sales.ss_sold_date_sk,
	web_sales.ws_ext_discount_amt,
	store_sales.ss_addr_sk,
	web_sales.ws_ext_wholesale_cost,
	store_sales.ss_item_sk,
	store_sales.ss_coupon_amt,
	item.i_item_sk,
	store_sales.ss_sold_time_sk,
	web_sales.ws_net_profit,
	web_sales.ws_list_price,
	item.i_wholesale_cost,
	web_sales.ws_sold_date_sk,
	web_sales.ws_ship_addr_sk,
	date_dim.d_day_name,
	item.i_category_id,
	time_dim.t_hour,
	store_sales.ss_ext_tax,
	web_sales.ws_bill_customer_sk,
	time_dim.t_time,
	web_sales.ws_item_sk,
	date_dim.d_dow
FROM
	date_dim,
	store_sales,
	web_sales,
	time_dim,
	item
WHERE
	web_sales.ws_sold_time_sk = time_dim.t_time_sk
	AND item.i_item_sk = web_sales.ws_item_sk
	AND web_sales.ws_sold_date_sk = date_dim.d_date_sk
	AND (((time_dim.t_meal_time = 'breakfast') OR (time_dim.t_meal_time = 'dinner')))
	AND item.i_manager_id = 1
	AND store_sales.ss_sold_date_sk = date_dim.d_date_sk
	AND date_dim.d_moy = 11
	AND date_dim.d_year = 1999;