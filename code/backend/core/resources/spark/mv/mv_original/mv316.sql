SELECT
	web_returns.wr_returning_customer_sk,
	catalog_returns.cr_item_sk,
	catalog_sales.cs_ship_customer_sk,
	web_sales.ws_ship_mode_sk,
	store_sales.ss_hdemo_sk,
	web_returns.wr_item_sk,
	web_sales.ws_order_number,
	catalog_sales.cs_catalog_page_sk,
	date_dim.d_qoy,
	web_returns.wr_refunded_cdemo_sk,
	catalog_sales.cs_call_center_sk,
	catalog_returns.cr_reversed_charge,
	web_sales.ws_ship_hdemo_sk,
	web_returns.wr_refunded_addr_sk,
	web_sales.ws_web_site_sk,
	store_sales.ss_list_price,
	web_sales.ws_sold_time_sk,
	store_sales.ss_store_sk,
	catalog_sales.cs_bill_cdemo_sk,
	store_sales.ss_ext_wholesale_cost,
	catalog_returns.cr_order_number,
	web_sales.ws_sales_price,
	catalog_sales.cs_bill_addr_sk,
	store_returns.sr_reason_sk,
	web_sales.ws_bill_addr_sk,
	date_dim.d_moy,
	web_returns.wr_returning_addr_sk,
	store_sales.ss_customer_sk,
	date_dim.d_week_seq,
	store_returns.sr_return_quantity,
	web_sales.ws_ext_ship_cost,
	catalog_sales.cs_item_sk,
	web_returns.wr_fee,
	catalog_sales.cs_bill_customer_sk,
	store_returns.sr_cdemo_sk,
	web_sales.ws_ship_customer_sk,
	store_returns.sr_customer_sk,
	web_returns.wr_return_amt,
	catalog_returns.cr_store_credit,
	date_dim.d_quarter_name,
	web_sales.ws_wholesale_cost,
	web_returns.wr_net_loss,
	web_returns.wr_web_page_sk,
	store_sales.ss_sales_price,
	web_returns.wr_reason_sk,
	catalog_returns.cr_call_center_sk,
	store_returns.sr_net_loss,
	catalog_returns.cr_refunded_cash,
	catalog_sales.cs_sold_time_sk,
	store_sales.ss_ext_discount_amt,
	store_sales.ss_promo_sk,
	catalog_sales.cs_sales_price,
	store_sales.ss_wholesale_cost,
	catalog_sales.cs_quantity,
	web_returns.wr_returned_date_sk,
	catalog_returns.cr_returning_customer_sk,
	catalog_returns.cr_returned_date_sk,
	web_sales.ws_ext_sales_price,
	web_returns.wr_returning_cdemo_sk,
	catalog_returns.cr_catalog_page_sk,
	catalog_sales.cs_coupon_amt,
	date_dim.d_dom,
	catalog_sales.cs_net_paid_inc_tax,
	store_sales.ss_net_paid,
	store_sales.ss_quantity,
	catalog_sales.cs_ext_discount_amt,
	date_dim.d_month_seq,
	web_sales.ws_ship_date_sk,
	store_sales.ss_ext_sales_price,
	catalog_sales.cs_ext_ship_cost,
	date_dim.d_date_sk,
	web_sales.ws_promo_sk,
	store_sales.ss_ticket_number,
	store_sales.ss_cdemo_sk,
	date_dim.d_date,
	store_returns.sr_ticket_number,
	catalog_sales.cs_order_number,
	catalog_sales.cs_wholesale_cost,
	catalog_sales.cs_ext_sales_price,
	web_sales.ws_ext_list_price,
	catalog_sales.cs_net_paid,
	catalog_returns.cr_returning_addr_sk,
	catalog_returns.cr_net_loss,
	web_sales.ws_warehouse_sk,
	catalog_sales.cs_ext_wholesale_cost,
	web_sales.ws_quantity,
	catalog_sales.cs_ship_date_sk,
	web_returns.wr_return_quantity,
	web_sales.ws_web_page_sk,
	store_sales.ss_ext_list_price,
	date_dim.d_year,
	web_sales.ws_net_paid,
	catalog_sales.cs_promo_sk,
	catalog_sales.cs_ship_mode_sk,
	web_returns.wr_refunded_cash,
	catalog_sales.cs_ext_list_price,
	store_sales.ss_net_profit,
	store_sales.ss_sold_date_sk,
	catalog_returns.cr_return_quantity,
	web_sales.ws_ext_discount_amt,
	store_sales.ss_addr_sk,
	web_sales.ws_ext_wholesale_cost,
	catalog_returns.cr_return_amount,
	catalog_sales.cs_bill_hdemo_sk,
	store_sales.ss_item_sk,
	catalog_returns.cr_return_amt_inc_tax,
	store_sales.ss_coupon_amt,
	store_sales.ss_sold_time_sk,
	catalog_sales.cs_warehouse_sk,
	web_sales.ws_list_price,
	web_sales.ws_sold_date_sk,
	store_returns.sr_returned_date_sk,
	web_sales.ws_ship_addr_sk,
	catalog_sales.cs_list_price,
	store_returns.sr_item_sk,
	web_returns.wr_order_number,
	catalog_sales.cs_net_profit,
	catalog_sales.cs_ship_addr_sk,
	store_returns.sr_return_amt,
	date_dim.d_day_name,
	store_sales.ss_ext_tax,
	web_sales.ws_bill_customer_sk,
	store_returns.sr_store_sk,
	web_sales.ws_net_profit,
	web_sales.ws_item_sk,
	date_dim.d_dow,
	catalog_sales.cs_sold_date_sk
FROM
	date_dim,
	store_sales,
	store_returns,
	web_sales,
	web_returns,
	catalog_returns,
	catalog_sales
WHERE
	web_sales.ws_item_sk = web_returns.wr_item_sk
	AND web_sales.ws_order_number = web_returns.wr_order_number
	AND catalog_sales.cs_item_sk = catalog_returns.cr_item_sk
	AND catalog_sales.cs_order_number = catalog_returns.cr_order_number
	AND store_sales.ss_ticket_number = store_returns.sr_ticket_number
	AND store_sales.ss_item_sk = store_returns.sr_item_sk
	AND store_sales.ss_customer_sk = web_sales.ws_bill_customer_sk
	AND store_sales.ss_customer_sk = catalog_sales.cs_bill_customer_sk
	AND store_sales.ss_item_sk = web_sales.ws_item_sk
	AND store_sales.ss_item_sk = catalog_sales.cs_item_sk
	AND store_sales.ss_sold_date_sk = date_dim.d_date_sk
	AND web_sales.ws_sold_date_sk = date_dim.d_date_sk
	AND catalog_sales.cs_sold_date_sk = date_dim.d_date_sk
	AND date_dim.d_year = 2000