SELECT
	item.i_product_name,
	item.i_category_id,
	item.i_brand,
	store_returns.sr_returned_date_sk,
	item.i_manufact,
	store_returns.sr_return_quantity,
	store_returns.sr_return_amt,
	item.i_current_price,
	store_returns.sr_item_sk,
	item.i_wholesale_cost,
	item.i_item_id,
	item.i_manager_id,
	store_returns.sr_net_loss,
	item.i_class_id,
	item.i_manufact_id,
	store_returns.sr_reason_sk,
	store_returns.sr_cdemo_sk,
	store_returns.sr_store_sk,
	item.i_color,
	item.i_size,
	store_returns.sr_ticket_number,
	item.i_item_desc,
	item.i_class,
	item.i_units,
	item.i_category,
	store_returns.sr_customer_sk,
	item.i_item_sk,
	item.i_brand_id
FROM
	store_returns,
	item
WHERE
	store_returns.sr_item_sk = item.i_item_sk