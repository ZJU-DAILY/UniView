SELECT
	catalog_sales.cs_ship_customer_sk,
	web_sales.ws_ship_mode_sk,
	item.i_brand_id,
	web_sales.ws_order_number,
	catalog_sales.cs_catalog_page_sk,
	date_dim.d_qoy,
	catalog_sales.cs_call_center_sk,
	web_sales.ws_ship_hdemo_sk,
	web_sales.ws_web_site_sk,
	web_sales.ws_sold_time_sk,
	item.i_manager_id,
	catalog_sales.cs_bill_cdemo_sk,
	item.i_class,
	web_sales.ws_sales_price,
	catalog_sales.cs_bill_addr_sk,
	item.i_item_id,
	date_dim.d_moy,
	web_sales.ws_bill_addr_sk,
	item.i_current_price,
	date_dim.d_week_seq,
	web_sales.ws_ext_ship_cost,
	catalog_sales.cs_item_sk,
	catalog_sales.cs_bill_customer_sk,
	item.i_manufact,
	item.i_units,
	web_sales.ws_ship_customer_sk,
	date_dim.d_quarter_name,
	web_sales.ws_wholesale_cost,
	item.i_product_name,
	catalog_sales.cs_sold_time_sk,
	item.i_color,
	catalog_sales.cs_sales_price,
	web_sales.ws_ext_sales_price,
	catalog_sales.cs_quantity,
	catalog_sales.cs_coupon_amt,
	date_dim.d_dom,
	item.i_manufact_id,
	catalog_sales.cs_net_paid_inc_tax,
	catalog_sales.cs_ext_discount_amt,
	item.i_brand,
	date_dim.d_month_seq,
	web_sales.ws_ship_date_sk,
	catalog_sales.cs_ext_ship_cost,
	date_dim.d_date_sk,
	web_sales.ws_promo_sk,
	date_dim.d_date,
	item.i_class_id,
	item.i_item_desc,
	catalog_sales.cs_order_number,
	catalog_sales.cs_wholesale_cost,
	catalog_sales.cs_ext_sales_price,
	item.i_category,
	web_sales.ws_ext_list_price,
	catalog_sales.cs_net_paid,
	item.i_size,
	web_sales.ws_warehouse_sk,
	catalog_sales.cs_ext_wholesale_cost,
	web_sales.ws_quantity,
	catalog_sales.cs_ship_date_sk,
	web_sales.ws_web_page_sk,
	date_dim.d_year,
	web_sales.ws_net_paid,
	catalog_sales.cs_promo_sk,
	catalog_sales.cs_ship_mode_sk,
	catalog_sales.cs_ext_list_price,
	web_sales.ws_ext_discount_amt,
	web_sales.ws_ext_wholesale_cost,
	catalog_sales.cs_bill_hdemo_sk,
	item.i_item_sk,
	catalog_sales.cs_warehouse_sk,
	web_sales.ws_list_price,
	item.i_wholesale_cost,
	web_sales.ws_sold_date_sk,
	web_sales.ws_ship_addr_sk,
	catalog_sales.cs_list_price,
	catalog_sales.cs_net_profit,
	catalog_sales.cs_ship_addr_sk,
	item.i_category_id,
	date_dim.d_day_name,
	web_sales.ws_bill_customer_sk,
	web_sales.ws_net_profit,
	web_sales.ws_item_sk,
	date_dim.d_dow,
	catalog_sales.cs_sold_date_sk
FROM
	date_dim,
	web_sales,
	catalog_sales,
	item
WHERE
	(((date_dim.d_year >= 1999) AND (date_dim.d_year <= 2001)))
	AND catalog_sales.cs_sold_date_sk = date_dim.d_date_sk
	AND web_sales.ws_sold_date_sk = date_dim.d_date_sk
	AND web_sales.ws_item_sk = item.i_item_sk
	AND catalog_sales.cs_item_sk = item.i_item_sk