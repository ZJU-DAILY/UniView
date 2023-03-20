SELECT
	catalog_sales.cs_ship_customer_sk,
	web_sales.ws_ship_mode_sk,
	store_sales.ss_hdemo_sk,
	item.i_brand_id,
	catalog_sales.cs_catalog_page_sk,
	web_sales.ws_order_number,
	date_dim.d_qoy,
	catalog_sales.cs_call_center_sk,
	web_sales.ws_ship_hdemo_sk,
	web_sales.ws_web_site_sk,
	store_sales.ss_list_price,
	web_sales.ws_sold_time_sk,
	item.i_manager_id,
	store_sales.ss_store_sk,
	catalog_sales.cs_bill_cdemo_sk,
	item.i_class,
	store_sales.ss_ext_wholesale_cost,
	web_sales.ws_sales_price,
	catalog_sales.cs_bill_addr_sk,
	item.i_item_id,
	web_sales.ws_bill_addr_sk,
	date_dim.d_moy,
	item.i_current_price,
	store_sales.ss_customer_sk,
	date_dim.d_week_seq,
	web_sales.ws_ext_ship_cost,
	catalog_sales.cs_item_sk,
	catalog_sales.cs_bill_customer_sk,
	item.i_manufact,
	item.i_units,
	web_sales.ws_ship_customer_sk,
	date_dim.d_quarter_name,
	web_sales.ws_wholesale_cost,
	store_sales.ss_sales_price,
	item.i_product_name,
	catalog_sales.cs_sold_time_sk,
	store_sales.ss_ext_discount_amt,
	store_sales.ss_promo_sk,
	catalog_sales.cs_sales_price,
	store_sales.ss_wholesale_cost,
	catalog_sales.cs_quantity,
	catalog_sales.cs_coupon_amt,
	web_sales.ws_ext_sales_price,
	item.i_color,
	date_dim.d_dom,
	item.i_manufact_id,
	catalog_sales.cs_net_paid_inc_tax,
	catalog_sales.cs_ext_discount_amt,
	store_sales.ss_net_paid,
	store_sales.ss_quantity,
	item.i_brand,
	date_dim.d_month_seq,
	web_sales.ws_ship_date_sk,
	store_sales.ss_ext_sales_price,
	catalog_sales.cs_ext_ship_cost,
	date_dim.d_date_sk,
	web_sales.ws_promo_sk,
	store_sales.ss_ticket_number,
	store_sales.ss_cdemo_sk,
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
	catalog_sales.cs_ext_wholesale_cost,
	web_sales.ws_warehouse_sk,
	catalog_sales.cs_ship_date_sk,
	web_sales.ws_quantity,
	web_sales.ws_web_page_sk,
	store_sales.ss_ext_list_price,
	date_dim.d_year,
	web_sales.ws_net_paid,
	catalog_sales.cs_promo_sk,
	catalog_sales.cs_ship_mode_sk,
	catalog_sales.cs_ext_list_price,
	store_sales.ss_net_profit,
	store_sales.ss_sold_date_sk,
	web_sales.ws_ext_discount_amt,
	store_sales.ss_addr_sk,
	web_sales.ws_ext_wholesale_cost,
	catalog_sales.cs_bill_hdemo_sk,
	store_sales.ss_item_sk,
	store_sales.ss_coupon_amt,
	item.i_item_sk,
	store_sales.ss_sold_time_sk,
	catalog_sales.cs_warehouse_sk,
	web_sales.ws_list_price,
	item.i_wholesale_cost,
	web_sales.ws_sold_date_sk,
	web_sales.ws_ship_addr_sk,
	catalog_sales.cs_list_price,
	catalog_sales.cs_net_profit,
	catalog_sales.cs_ship_addr_sk,
	date_dim.d_day_name,
	item.i_category_id,
	store_sales.ss_ext_tax,
	web_sales.ws_bill_customer_sk,
	web_sales.ws_net_profit,
	web_sales.ws_item_sk,
	date_dim.d_dow,
	catalog_sales.cs_sold_date_sk
FROM
	date_dim,
	store_sales,
	web_sales,
	catalog_sales,
	item
WHERE
	store_sales.ss_item_sk = item.i_item_sk
	AND store_sales.ss_sold_date_sk = date_dim.d_date_sk
	AND catalog_sales.cs_item_sk = item.i_item_sk
	AND web_sales.ws_item_sk = item.i_item_sk
	AND web_sales.ws_sold_date_sk = date_dim.d_date_sk
	AND catalog_sales.cs_sold_date_sk = date_dim.d_date_sk
	AND date_dim.d_date = cast('1970-01-01' as date) + interval 10959 days