CREATE MATERIALIZED VIEW IF NOT EXISTS mv43
PARTITIONED BY (ws_sold_date_sk)
AS
SELECT
	item.i_brand,
	web_sales.ws_ship_date_sk,
	web_sales.ws_ship_mode_sk,
	item.i_brand_id,
	web_sales.ws_order_number,
	web_sales.ws_promo_sk,
	web_sales.ws_ship_hdemo_sk,
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
	web_sales.ws_quantity,
	item.i_current_price,
	web_sales.ws_bill_addr_sk,
	web_sales.ws_web_page_sk,
	web_sales.ws_net_paid,
	web_sales.ws_ext_ship_cost,
	item.i_manufact,
	item.i_units,
	web_sales.ws_ship_customer_sk,
	web_sales.ws_ext_discount_amt,
	web_sales.ws_ext_wholesale_cost,
	item.i_manufact_id,
	web_sales.ws_wholesale_cost,
	item.i_item_sk,
	item.i_product_name,
	web_sales.ws_list_price,
	item.i_wholesale_cost,
	web_sales.ws_sold_date_sk,
	item.i_color,
	web_sales.ws_ext_sales_price,
	web_sales.ws_ship_addr_sk,
	item.i_category_id,
	web_sales.ws_bill_customer_sk,
	web_sales.ws_net_profit,
	web_sales.ws_item_sk
FROM
	web_sales,
	item
WHERE
	(((item.i_category = 'Sports') OR (item.i_category = 'Books') OR (item.i_category = 'Home')))
	AND web_sales.ws_item_sk = item.i_item_sk
DISTRIBUTE BY ws_sold_date_sk;