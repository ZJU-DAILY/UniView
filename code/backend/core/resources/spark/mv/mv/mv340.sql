CREATE MATERIALIZED VIEW IF NOT EXISTS mv340
PARTITIONED BY (wr_returned_date_sk)
AS
SELECT
	web_returns.wr_returning_customer_sk,
	item.i_brand,
	date_dim.d_month_seq,
	web_returns.wr_item_sk,
	item.i_brand_id,
	date_dim.d_date_sk,
	date_dim.d_qoy,
	web_returns.wr_refunded_cdemo_sk,
	web_returns.wr_refunded_addr_sk,
	date_dim.d_date,
	item.i_manager_id,
	item.i_class_id,
	item.i_item_desc,
	item.i_class,
	item.i_category,
	item.i_size,
	item.i_item_id,
	date_dim.d_moy,
	web_returns.wr_return_quantity,
	web_returns.wr_returning_addr_sk,
	item.i_current_price,
	date_dim.d_week_seq,
	date_dim.d_year,
	web_returns.wr_fee,
	web_returns.wr_refunded_cash,
	item.i_manufact,
	item.i_units,
	web_returns.wr_return_amt,
	date_dim.d_quarter_name,
	web_returns.wr_net_loss,
	web_returns.wr_web_page_sk,
	item.i_item_sk,
	web_returns.wr_reason_sk,
	item.i_product_name,
	item.i_wholesale_cost,
	item.i_color,
	web_returns.wr_returned_date_sk,
	web_returns.wr_returning_cdemo_sk,
	web_returns.wr_order_number,
	date_dim.d_day_name,
	item.i_category_id,
	date_dim.d_dom,
	item.i_manufact_id,
	date_dim.d_dow
FROM
	date_dim,
	web_returns,
	item
WHERE
	web_returns.wr_item_sk = item.i_item_sk
	AND web_returns.wr_returned_date_sk = date_dim.d_date_sk
	AND (((cast(date_dim.d_date as string) = '2000-06-30') OR (cast(date_dim.d_date as string) = '2000-09-27') OR (cast(date_dim.d_date as string) = '2000-11-17')))
DISTRIBUTE BY wr_returned_date_sk;