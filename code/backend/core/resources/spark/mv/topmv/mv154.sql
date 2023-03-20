CREATE MATERIALIZED VIEW IF NOT EXISTS mv154
PARTITIONED BY (ss_sold_date_sk)
AS
SELECT
	date_dim.d_month_seq,
	store_sales.ss_hdemo_sk,
	store_sales.ss_ext_sales_price,
	date_dim.d_date_sk,
	date_dim.d_qoy,
	store_sales.ss_ticket_number,
	date_dim.d_date,
	store_sales.ss_cdemo_sk,
	store_sales.ss_list_price,
	store_sales.ss_store_sk,
	store_sales.ss_ext_wholesale_cost,
	date_dim.d_moy,
	store_sales.ss_customer_sk,
	date_dim.d_week_seq,
	store_sales.ss_ext_list_price,
	date_dim.d_year,
	store_sales.ss_net_profit,
	store_sales.ss_sold_date_sk,
	store_sales.ss_addr_sk,
	date_dim.d_quarter_name,
	store_sales.ss_sales_price,
	store_sales.ss_item_sk,
	store_sales.ss_coupon_amt,
	store_sales.ss_sold_time_sk,
	store_sales.ss_ext_discount_amt,
	store_sales.ss_promo_sk,
	store_sales.ss_wholesale_cost,
	date_dim.d_day_name,
	date_dim.d_dom,
	store_sales.ss_ext_tax,
	store_sales.ss_net_paid,
	store_sales.ss_quantity,
	date_dim.d_dow
FROM
	date_dim,
	store_sales
WHERE
	store_sales.ss_sold_date_sk = date_dim.d_date_sk
	AND (((date_dim.d_month_seq >= 1200) AND (date_dim.d_month_seq <= 1211)))
DISTRIBUTE BY ss_sold_date_sk;