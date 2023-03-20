CREATE MATERIALIZED VIEW IF NOT EXISTS mv101
PARTITIONED BY (ss_sold_date_sk)
AS
SELECT
	web_sales.ws_ship_mode_sk,
	store_sales.ss_hdemo_sk,
	web_sales.ws_order_number,
	date_dim.d_qoy,
	web_sales.ws_ship_hdemo_sk,
	web_sales.ws_web_site_sk,
	store_sales.ss_list_price,
	web_sales.ws_sold_time_sk,
	store_sales.ss_store_sk,
	store_sales.ss_ext_wholesale_cost,
	web_sales.ws_sales_price,
	date_dim.d_moy,
	web_sales.ws_bill_addr_sk,
	store_sales.ss_customer_sk,
	date_dim.d_week_seq,
	web_sales.ws_ext_ship_cost,
	web_sales.ws_ship_customer_sk,
	date_dim.d_quarter_name,
	web_sales.ws_wholesale_cost,
	store_sales.ss_sales_price,
	store_sales.ss_ext_discount_amt,
	store_sales.ss_promo_sk,
	store_sales.ss_wholesale_cost,
	web_sales.ws_ext_sales_price,
	date_dim.d_dom,
	store_sales.ss_net_paid,
	store_sales.ss_quantity,
	date_dim.d_month_seq,
	web_sales.ws_ship_date_sk,
	store_sales.ss_ext_sales_price,
	date_dim.d_date_sk,
	web_sales.ws_promo_sk,
	store_sales.ss_ticket_number,
	store_sales.ss_cdemo_sk,
	date_dim.d_date,
	web_sales.ws_ext_list_price,
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
	store_sales.ss_coupon_amt,
	store_sales.ss_sold_time_sk,
	web_sales.ws_list_price,
	web_sales.ws_sold_date_sk,
	web_sales.ws_ship_addr_sk,
	date_dim.d_day_name,
	store_sales.ss_ext_tax,
	web_sales.ws_bill_customer_sk,
	web_sales.ws_net_profit,
	date_dim.d_dow
FROM
	web_sales SEMI JOIN item ON (web_sales.ws_item_sk = item.i_item_sk)	Join date_dim
	Join store_sales
WHERE
	date_dim.d_moy = 2
	AND (((date_dim.d_year = 2000) OR (date_dim.d_year = 2001) OR (date_dim.d_year = 2002) OR (date_dim.d_year = 2003)))
	AND date_dim.d_year = 2000
	AND store_sales.ss_sold_date_sk = date_dim.d_date_sk
	AND store_sales.ss_item_sk = item.i_item_sk
DISTRIBUTE BY ss_sold_date_sk;