CREATE MATERIALIZED VIEW IF NOT EXISTS mv98
PARTITIONED BY (ss_sold_date_sk)
AS
SELECT
	catalog_sales.cs_ship_customer_sk,
	store_sales.ss_hdemo_sk,
	catalog_sales.cs_catalog_page_sk,
	date_dim.d_qoy,
	catalog_sales.cs_call_center_sk,
	store_sales.ss_list_price,
	store_sales.ss_store_sk,
	catalog_sales.cs_bill_cdemo_sk,
	store_sales.ss_ext_wholesale_cost,
	catalog_sales.cs_bill_addr_sk,
	date_dim.d_moy,
	store_sales.ss_customer_sk,
	date_dim.d_week_seq,
	catalog_sales.cs_bill_customer_sk,
	date_dim.d_quarter_name,
	store_sales.ss_sales_price,
	catalog_sales.cs_sold_time_sk,
	store_sales.ss_ext_discount_amt,
	store_sales.ss_promo_sk,
	catalog_sales.cs_sales_price,
	catalog_sales.cs_quantity,
	store_sales.ss_wholesale_cost,
	catalog_sales.cs_coupon_amt,
	date_dim.d_dom,
	catalog_sales.cs_net_paid_inc_tax,
	store_sales.ss_net_paid,
	catalog_sales.cs_ext_discount_amt,
	store_sales.ss_quantity,
	date_dim.d_month_seq,
	store_sales.ss_ext_sales_price,
	catalog_sales.cs_ext_ship_cost,
	date_dim.d_date_sk,
	store_sales.ss_ticket_number,
	store_sales.ss_cdemo_sk,
	date_dim.d_date,
	catalog_sales.cs_order_number,
	catalog_sales.cs_wholesale_cost,
	catalog_sales.cs_ext_sales_price,
	catalog_sales.cs_net_paid,
	catalog_sales.cs_ext_wholesale_cost,
	catalog_sales.cs_ship_date_sk,
	store_sales.ss_ext_list_price,
	date_dim.d_year,
	catalog_sales.cs_promo_sk,
	catalog_sales.cs_ship_mode_sk,
	catalog_sales.cs_ext_list_price,
	store_sales.ss_net_profit,
	store_sales.ss_sold_date_sk,
	store_sales.ss_addr_sk,
	catalog_sales.cs_bill_hdemo_sk,
	store_sales.ss_coupon_amt,
	store_sales.ss_sold_time_sk,
	catalog_sales.cs_warehouse_sk,
	catalog_sales.cs_list_price,
	catalog_sales.cs_net_profit,
	catalog_sales.cs_ship_addr_sk,
	date_dim.d_day_name,
	store_sales.ss_ext_tax,
	date_dim.d_dow,
	catalog_sales.cs_sold_date_sk
FROM
	catalog_sales SEMI JOIN item ON (catalog_sales.cs_item_sk = item.i_item_sk)	Join date_dim
	Join store_sales
WHERE
	date_dim.d_moy = 2
	AND (((date_dim.d_year = 2000) OR (date_dim.d_year = 2001) OR (date_dim.d_year = 2002) OR (date_dim.d_year = 2003)))
	AND date_dim.d_year = 2000
	AND store_sales.ss_sold_date_sk = date_dim.d_date_sk
	AND store_sales.ss_item_sk = item.i_item_sk
DISTRIBUTE BY ss_sold_date_sk;