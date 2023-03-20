CREATE MATERIALIZED VIEW IF NOT EXISTS mv84
PARTITIONED BY (ss_sold_date_sk)
AS
SELECT
	item.i_brand,
	date_dim.d_month_seq,
	store_sales.ss_hdemo_sk,
	store_sales.ss_ext_sales_price,
	item.i_brand_id,
	date_dim.d_date_sk,
	date_dim.d_qoy,
	store_sales.ss_ticket_number,
	store_sales.ss_cdemo_sk,
	date_dim.d_date,
	store_sales.ss_list_price,
	item.i_manager_id,
	item.i_class_id,
	store_sales.ss_store_sk,
	item.i_item_desc,
	item.i_class,
	store_sales.ss_ext_wholesale_cost,
	item.i_category,
	item.i_size,
	item.i_item_id,
	date_dim.d_moy,
	item.i_current_price,
	store_sales.ss_ext_list_price,
	date_dim.d_week_seq,
	store_sales.ss_customer_sk,
	date_dim.d_year,
	item.i_manufact,
	store_sales.ss_net_profit,
	item.i_units,
	store_sales.ss_sold_date_sk,
	store_sales.ss_addr_sk,
	date_dim.d_quarter_name,
	store_sales.ss_sales_price,
	store_sales.ss_item_sk,
	store_sales.ss_coupon_amt,
	item.i_item_sk,
	store_sales.ss_sold_time_sk,
	item.i_product_name,
	item.i_wholesale_cost,
	store_sales.ss_ext_discount_amt,
	store_sales.ss_promo_sk,
	item.i_color,
	store_sales.ss_wholesale_cost,
	item.i_category_id,
	date_dim.d_day_name,
	date_dim.d_dom,
	store_sales.ss_ext_tax,
	item.i_manufact_id,
	store_sales.ss_net_paid,
	store_sales.ss_quantity,
	date_dim.d_dow
FROM
	date_dim,
	store_sales,
	item
WHERE
	store_sales.ss_item_sk = item.i_item_sk
	AND date_dim.d_date_sk = store_sales.ss_sold_date_sk
	AND (item.i_manager_id = 8 OR item.i_manager_id = 1 OR item.i_manager_id = 28)
	AND (date_dim.d_year = 1998 OR date_dim.d_year = 2000 OR date_dim.d_year = 1999)
	AND date_dim.d_moy = 11
DISTRIBUTE BY ss_sold_date_sk;