CREATE MATERIALIZED VIEW IF NOT EXISTS mv324
PARTITIONED BY (ws_sold_date_sk)
AS
SELECT
	web_returns.wr_returning_customer_sk,
	date_dim.d_month_seq,
	web_sales.ws_ship_date_sk,
	web_sales.ws_ship_mode_sk,
	web_returns.wr_item_sk,
	date_dim.d_date_sk,
	web_sales.ws_order_number,
	date_dim.d_qoy,
	web_returns.wr_refunded_cdemo_sk,
	web_sales.ws_promo_sk,
	web_sales.ws_ship_hdemo_sk,
	web_returns.wr_refunded_addr_sk,
	date_dim.d_date,
	web_sales.ws_web_site_sk,
	web_sales.ws_sold_time_sk,
	web_sales.ws_sales_price,
	web_sales.ws_ext_list_price,
	web_sales.ws_warehouse_sk,
	web_sales.ws_quantity,
	date_dim.d_moy,
	web_sales.ws_bill_addr_sk,
	web_returns.wr_return_quantity,
	web_sales.ws_web_page_sk,
	date_dim.d_week_seq,
	web_returns.wr_returning_addr_sk,
	date_dim.d_year,
	web_sales.ws_net_paid,
	web_sales.ws_ext_ship_cost,
	web_returns.wr_fee,
	web_returns.wr_refunded_cash,
	web_sales.ws_ship_customer_sk,
	web_returns.wr_return_amt,
	web_sales.ws_ext_discount_amt,
	web_sales.ws_ext_wholesale_cost,
	web_returns.wr_net_loss,
	date_dim.d_quarter_name,
	web_sales.ws_wholesale_cost,
	web_returns.wr_web_page_sk,
	web_returns.wr_reason_sk,
	web_sales.ws_list_price,
	web_sales.ws_sold_date_sk,
	web_sales.ws_ext_sales_price,
	web_sales.ws_ship_addr_sk,
	web_returns.wr_returned_date_sk,
	web_returns.wr_returning_cdemo_sk,
	web_returns.wr_order_number,
	date_dim.d_day_name,
	date_dim.d_dom,
	web_sales.ws_bill_customer_sk,
	web_sales.ws_net_profit,
	web_sales.ws_item_sk,
	date_dim.d_dow
FROM
	web_sales LEFT OUTER JOIN web_returns ON (web_sales.ws_order_number = web_returns.wr_order_number AND web_sales.ws_item_sk = web_returns.wr_item_sk)	Join date_dim
WHERE
	web_sales.ws_sold_date_sk = date_dim.d_date_sk
	AND (((date_dim.d_date >= cast('1970-01-01' as date) + interval 11192 days) AND (date_dim.d_date <= cast('1970-01-01' as date) + interval 11222 days)))
DISTRIBUTE BY ws_sold_date_sk;