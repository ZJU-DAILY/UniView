CREATE MATERIALIZED VIEW IF NOT EXISTS mv36
PARTITIONED BY (ws_sold_date_sk)
AS
SELECT
	date_dim.d_month_seq,
	web_sales.ws_ship_date_sk,
	web_sales.ws_ship_mode_sk,
	date_dim.d_date_sk,
	web_sales.ws_order_number,
	date_dim.d_qoy,
	web_sales.ws_promo_sk,
	web_sales.ws_ship_hdemo_sk,
	date_dim.d_date,
	web_sales.ws_web_site_sk,
	web_sales.ws_sold_time_sk,
	web_sales.ws_sales_price,
	web_sales.ws_ext_list_price,
	web_sales.ws_warehouse_sk,
	web_sales.ws_quantity,
	date_dim.d_moy,
	web_sales.ws_bill_addr_sk,
	web_sales.ws_web_page_sk,
	date_dim.d_week_seq,
	date_dim.d_year,
	web_sales.ws_net_paid,
	web_sales.ws_ext_ship_cost,
	web_sales.ws_ship_customer_sk,
	web_sales.ws_ext_discount_amt,
	web_sales.ws_ext_wholesale_cost,
	date_dim.d_quarter_name,
	web_sales.ws_wholesale_cost,
	web_sales.ws_list_price,
	web_sales.ws_sold_date_sk,
	web_sales.ws_ext_sales_price,
	web_sales.ws_ship_addr_sk,
	date_dim.d_day_name,
	date_dim.d_dom,
	web_sales.ws_bill_customer_sk,
	web_sales.ws_net_profit,
	web_sales.ws_item_sk,
	date_dim.d_dow
FROM
	date_dim,
	web_sales
WHERE
	web_sales.ws_sold_date_sk = date_dim.d_date_sk
	AND (date_dim.d_year = 2002 OR date_dim.d_year = 1998 OR date_dim.d_year = 2001 OR date_dim.d_year = 1999)
	AND (date_dim.d_moy <= 4 OR date_dim.d_moy = 5 OR date_dim.d_moy = 9 OR date_dim.d_moy = 11)
DISTRIBUTE BY ws_sold_date_sk;