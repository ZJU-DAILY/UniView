CREATE MATERIALIZED VIEW IF NOT EXISTS mv320
PARTITIONED BY (ss_sold_date_sk)
AS
SELECT
	date_dim.d_month_seq,
	store_sales.ss_hdemo_sk,
	store_sales.ss_ext_sales_price,
	date_dim.d_date_sk,
	date_dim.d_qoy,
	store_sales.ss_ticket_number,
	store_sales.ss_cdemo_sk,
	date_dim.d_date,
	store_sales.ss_list_price,
	store_returns.sr_ticket_number,
	store_sales.ss_store_sk,
	store_sales.ss_ext_wholesale_cost,
	store_returns.sr_reason_sk,
	date_dim.d_moy,
	store_sales.ss_customer_sk,
	date_dim.d_week_seq,
	store_sales.ss_ext_list_price,
	store_returns.sr_return_quantity,
	date_dim.d_year,
	store_returns.sr_cdemo_sk,
	store_sales.ss_net_profit,
	store_sales.ss_sold_date_sk,
	store_returns.sr_customer_sk,
	store_sales.ss_addr_sk,
	date_dim.d_quarter_name,
	store_sales.ss_sales_price,
	store_sales.ss_item_sk,
	store_sales.ss_coupon_amt,
	store_sales.ss_sold_time_sk,
	store_returns.sr_net_loss,
	store_returns.sr_returned_date_sk,
	store_sales.ss_ext_discount_amt,
	store_sales.ss_promo_sk,
	store_sales.ss_wholesale_cost,
	store_returns.sr_item_sk,
	store_returns.sr_return_amt,
	date_dim.d_day_name,
	date_dim.d_dom,
	store_sales.ss_ext_tax,
	store_returns.sr_store_sk,
	store_sales.ss_net_paid,
	store_sales.ss_quantity,
	date_dim.d_dow
FROM
	store_sales LEFT OUTER JOIN store_returns ON (store_sales.ss_item_sk = store_returns.sr_item_sk AND store_sales.ss_ticket_number = store_returns.sr_ticket_number)	Join date_dim
WHERE
	store_sales.ss_sold_date_sk = date_dim.d_date_sk
	AND (((date_dim.d_date >= cast('1970-01-01' as date) + interval 11192 days) AND (date_dim.d_date <= cast('1970-01-01' as date) + interval 11222 days)))
DISTRIBUTE BY ss_sold_date_sk;