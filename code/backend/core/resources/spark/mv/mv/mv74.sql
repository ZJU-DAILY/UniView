CREATE MATERIALIZED VIEW IF NOT EXISTS mv74
AS
SELECT
	date_dim.d_month_seq,
	catalog_sales.cs_ship_customer_sk,
	catalog_sales.cs_ext_discount_amt,
	store_sales.ss_hdemo_sk,
	store_sales.ss_ext_sales_price,
	catalog_sales.cs_ext_ship_cost,
	date_dim.d_date_sk,
	catalog_sales.cs_catalog_page_sk,
	date_dim.d_qoy,
	catalog_sales.cs_call_center_sk,
	store_sales.ss_ticket_number,
	store_sales.ss_cdemo_sk,
	date_dim.d_date,
	store_sales.ss_list_price,
	store_returns.sr_ticket_number,
	store_sales.ss_store_sk,
	catalog_sales.cs_order_number,
	catalog_sales.cs_bill_cdemo_sk,
	catalog_sales.cs_ext_sales_price,
	store_sales.ss_ext_wholesale_cost,
	catalog_sales.cs_wholesale_cost,
	catalog_sales.cs_bill_addr_sk,
	catalog_sales.cs_net_paid,
	store_returns.sr_reason_sk,
	catalog_sales.cs_ext_wholesale_cost,
	catalog_sales.cs_ship_date_sk,
	date_dim.d_moy,
	store_sales.ss_customer_sk,
	date_dim.d_week_seq,
	store_sales.ss_ext_list_price,
	store_returns.sr_return_quantity,
	date_dim.d_year,
	catalog_sales.cs_promo_sk,
	catalog_sales.cs_item_sk,
	store_returns.sr_cdemo_sk,
	catalog_sales.cs_bill_customer_sk,
	catalog_sales.cs_ship_mode_sk,
	store_sales.ss_net_profit,
	catalog_sales.cs_ext_list_price,
	store_sales.ss_sold_date_sk,
	store_returns.sr_customer_sk,
	store_sales.ss_addr_sk,
	date_dim.d_quarter_name,
	catalog_sales.cs_bill_hdemo_sk,
	store_sales.ss_sales_price,
	store_sales.ss_item_sk,
	store_sales.ss_coupon_amt,
	store_sales.ss_sold_time_sk,
	catalog_sales.cs_warehouse_sk,
	store_returns.sr_net_loss,
	store_returns.sr_returned_date_sk,
	catalog_sales.cs_sold_time_sk,
	store_sales.ss_ext_discount_amt,
	store_sales.ss_promo_sk,
	catalog_sales.cs_sales_price,
	catalog_sales.cs_quantity,
	store_sales.ss_wholesale_cost,
	catalog_sales.cs_coupon_amt,
	catalog_sales.cs_list_price,
	store_returns.sr_item_sk,
	catalog_sales.cs_net_profit,
	catalog_sales.cs_ship_addr_sk,
	store_returns.sr_return_amt,
	date_dim.d_day_name,
	date_dim.d_dom,
	store_sales.ss_ext_tax,
	store_returns.sr_store_sk,
	catalog_sales.cs_net_paid_inc_tax,
	store_sales.ss_net_paid,
	store_sales.ss_quantity,
	date_dim.d_dow,
	catalog_sales.cs_sold_date_sk
FROM
	date_dim,
	store_sales,
	store_returns,
	catalog_sales
WHERE
	store_sales.ss_customer_sk = store_returns.sr_customer_sk
	AND store_returns.sr_customer_sk = catalog_sales.cs_bill_customer_sk
	AND store_sales.ss_ticket_number = store_returns.sr_ticket_number
	AND store_returns.sr_item_sk = catalog_sales.cs_item_sk
	AND store_sales.ss_item_sk = store_returns.sr_item_sk
	AND store_returns.sr_returned_date_sk = date_dim.d_date_sk
	AND store_sales.ss_sold_date_sk = date_dim.d_date_sk
	AND (((date_dim.d_quarter_name = '2001Q1') OR (date_dim.d_quarter_name = '2001Q2') OR (date_dim.d_quarter_name = '2001Q3')))
	AND date_dim.d_quarter_name = '2001Q1';