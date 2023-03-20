CREATE MATERIALIZED VIEW IF NOT EXISTS mv231
PARTITIONED BY (ws_sold_date_sk)
AS
SELECT
	warehouse.w_city,
	web_sales.ws_ship_date_sk,
	web_sales.ws_ship_mode_sk,
	web_sales.ws_order_number,
	web_sales.ws_promo_sk,
	web_sales.ws_ship_hdemo_sk,
	warehouse.w_warehouse_sq_ft,
	web_sales.ws_web_site_sk,
	warehouse.w_county,
	web_sales.ws_sold_time_sk,
	warehouse.w_state,
	web_sales.ws_sales_price,
	warehouse.w_warehouse_name,
	web_sales.ws_ext_list_price,
	warehouse.w_country,
	web_sales.ws_warehouse_sk,
	web_sales.ws_quantity,
	web_sales.ws_bill_addr_sk,
	web_sales.ws_web_page_sk,
	web_sales.ws_net_paid,
	web_sales.ws_ext_ship_cost,
	web_sales.ws_ship_customer_sk,
	web_sales.ws_ext_discount_amt,
	web_sales.ws_ext_wholesale_cost,
	web_sales.ws_wholesale_cost,
	web_sales.ws_list_price,
	web_sales.ws_sold_date_sk,
	web_sales.ws_ext_sales_price,
	warehouse.w_warehouse_sk,
	web_sales.ws_ship_addr_sk,
	web_sales.ws_bill_customer_sk,
	web_sales.ws_net_profit,
	web_sales.ws_item_sk
FROM
	web_sales,
	warehouse
WHERE
	web_sales.ws_warehouse_sk = warehouse.w_warehouse_sk
DISTRIBUTE BY ws_sold_date_sk;