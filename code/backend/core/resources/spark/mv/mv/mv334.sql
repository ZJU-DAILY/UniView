CREATE MATERIALIZED VIEW IF NOT EXISTS mv334
PARTITIONED BY (sr_returned_date_sk)
AS
SELECT
	item.i_brand,
	store_returns.sr_cdemo_sk,
	item.i_manufact,
	item.i_units,
	store_returns.sr_customer_sk,
	item.i_brand_id,
	item.i_item_sk,
	item.i_product_name,
	item.i_wholesale_cost,
	store_returns.sr_net_loss,
	store_returns.sr_ticket_number,
	store_returns.sr_returned_date_sk,
	item.i_class_id,
	item.i_manager_id,
	item.i_item_desc,
	item.i_color,
	item.i_class,
	store_returns.sr_item_sk,
	item.i_category,
	store_returns.sr_return_amt,
	item.i_size,
	item.i_category_id,
	store_returns.sr_reason_sk,
	item.i_item_id,
	item.i_current_price,
	item.i_manufact_id,
	store_returns.sr_store_sk,
	store_returns.sr_return_quantity
FROM
	store_returns,
	item
WHERE
	store_returns.sr_item_sk = item.i_item_sk
DISTRIBUTE BY sr_returned_date_sk;