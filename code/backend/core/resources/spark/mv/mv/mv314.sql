CREATE MATERIALIZED VIEW IF NOT EXISTS mv314
AS
SELECT
	web_returns.wr_returning_customer_sk,
	web_sales.ws_ship_mode_sk,
	store_sales.ss_hdemo_sk,
	web_returns.wr_item_sk,
	web_sales.ws_order_number,
	date_dim.d_qoy,
	web_returns.wr_refunded_cdemo_sk,
	web_sales.ws_ship_hdemo_sk,
	web_returns.wr_refunded_addr_sk,
	web_sales.ws_web_site_sk,
	store_sales.ss_list_price,
	web_sales.ws_sold_time_sk,
	store_sales.ss_store_sk,
	store_sales.ss_ext_wholesale_cost,
	web_sales.ws_sales_price,
	store_returns.sr_reason_sk,
	date_dim.d_moy,
	web_sales.ws_bill_addr_sk,
	web_returns.wr_returning_addr_sk,
	store_sales.ss_customer_sk,
	date_dim.d_week_seq,
	store_returns.sr_return_quantity,
	web_sales.ws_ext_ship_cost,
	store_returns.sr_cdemo_sk,
	web_returns.wr_fee,
	web_sales.ws_ship_customer_sk,
	store_returns.sr_customer_sk,
	web_returns.wr_return_amt,
	date_dim.d_quarter_name,
	web_sales.ws_wholesale_cost,
	store_sales.ss_sales_price,
	web_returns.wr_net_loss,
	web_returns.wr_web_page_sk,
	web_returns.wr_reason_sk,
	store_returns.sr_net_loss,
	store_sales.ss_ext_discount_amt,
	store_sales.ss_promo_sk,
	store_sales.ss_wholesale_cost,
	web_sales.ws_ext_sales_price,
	web_returns.wr_returned_date_sk,
	web_returns.wr_returning_cdemo_sk,
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
	store_returns.sr_ticket_number,
	web_sales.ws_ext_list_price,
	web_sales.ws_warehouse_sk,
	web_sales.ws_quantity,
	web_returns.wr_return_quantity,
	store_sales.ss_ext_list_price,
	web_sales.ws_web_page_sk,
	date_dim.d_year,
	web_sales.ws_net_paid,
	web_returns.wr_refunded_cash,
	store_sales.ss_net_profit,
	store_sales.ss_sold_date_sk,
	web_sales.ws_ext_discount_amt,
	store_sales.ss_addr_sk,
	web_sales.ws_ext_wholesale_cost,
	store_sales.ss_item_sk,
	store_sales.ss_coupon_amt,
	store_sales.ss_sold_time_sk,
	web_sales.ws_list_price,
	web_sales.ws_sold_date_sk,
	store_returns.sr_returned_date_sk,
	web_sales.ws_ship_addr_sk,
	store_returns.sr_item_sk,
	web_returns.wr_order_number,
	store_returns.sr_return_amt,
	date_dim.d_day_name,
	store_sales.ss_ext_tax,
	web_sales.ws_bill_customer_sk,
	store_returns.sr_store_sk,
	web_sales.ws_net_profit,
	web_sales.ws_item_sk,
	date_dim.d_dow
FROM
	date_dim,
	store_sales,
	store_returns,
	web_sales,
	web_returns
WHERE
	web_sales.ws_item_sk = web_returns.wr_item_sk
	AND web_sales.ws_order_number = web_returns.wr_order_number
	AND store_sales.ss_ticket_number = store_returns.sr_ticket_number
	AND store_sales.ss_item_sk = store_returns.sr_item_sk
	AND store_sales.ss_customer_sk = web_sales.ws_bill_customer_sk
	AND store_sales.ss_item_sk = web_sales.ws_item_sk
	AND store_sales.ss_sold_date_sk = date_dim.d_date_sk
	AND web_sales.ws_sold_date_sk = date_dim.d_date_sk
	AND date_dim.d_year = 2000;