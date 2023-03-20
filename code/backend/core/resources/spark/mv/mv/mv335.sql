CREATE MATERIALIZED VIEW IF NOT EXISTS mv335
PARTITIONED BY (sr_returned_date_sk)
AS
SELECT
	item.i_brand,
	date_dim.d_month_seq,
	item.i_brand_id,
	date_dim.d_date_sk,
	date_dim.d_qoy,
	date_dim.d_date,
	store_returns.sr_ticket_number,
	item.i_manager_id,
	item.i_class_id,
	item.i_item_desc,
	item.i_class,
	item.i_category,
	item.i_size,
	store_returns.sr_reason_sk,
	item.i_item_id,
	date_dim.d_moy,
	item.i_current_price,
	date_dim.d_week_seq,
	store_returns.sr_return_quantity,
	date_dim.d_year,
	store_returns.sr_cdemo_sk,
	item.i_manufact,
	item.i_units,
	store_returns.sr_customer_sk,
	date_dim.d_quarter_name,
	item.i_item_sk,
	item.i_product_name,
	item.i_wholesale_cost,
	store_returns.sr_net_loss,
	store_returns.sr_returned_date_sk,
	item.i_color,
	store_returns.sr_item_sk,
	store_returns.sr_return_amt,
	date_dim.d_day_name,
	item.i_category_id,
	date_dim.d_dom,
	item.i_manufact_id,
	store_returns.sr_store_sk,
	date_dim.d_dow
FROM
	date_dim,
	store_returns,
	item
WHERE
	store_returns.sr_item_sk = item.i_item_sk
	AND store_returns.sr_returned_date_sk = date_dim.d_date_sk
	AND (((cast(date_dim.d_date as string) = '2000-06-30') OR (cast(date_dim.d_date as string) = '2000-09-27') OR (cast(date_dim.d_date as string) = '2000-11-17')))
DISTRIBUTE BY sr_returned_date_sk;