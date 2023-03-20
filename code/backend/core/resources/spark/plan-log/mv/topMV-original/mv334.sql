SELECT
	date_dim.d_dow,
	item.i_manufact,
	store_returns.sr_returned_date_sk,
	store_returns.sr_return_amt,
	date_dim.d_quarter_name,
	date_dim.d_day_name,
	item.i_manager_id,
	store_returns.sr_net_loss,
	date_dim.d_qoy,
	item.i_class_id,
	date_dim.d_dom,
	store_returns.sr_cdemo_sk,
	item.i_color,
	item.i_size,
	item.i_units,
	store_returns.sr_customer_sk,
	date_dim.d_week_seq,
	date_dim.d_year,
	item.i_item_sk,
	item.i_brand_id,
	item.i_product_name,
	item.i_brand,
	store_returns.sr_return_quantity,
	item.i_current_price,
	date_dim.d_month_seq,
	store_returns.sr_item_sk,
	item.i_wholesale_cost,
	item.i_item_id,
	item.i_manufact_id,
	store_returns.sr_reason_sk,
	store_returns.sr_store_sk,
	store_returns.sr_ticket_number,
	item.i_item_desc,
	item.i_class,
	date_dim.d_moy,
	date_dim.d_date,
	item.i_category,
	date_dim.d_date_sk,
	item.i_category_id
FROM
	store_returns,
	item,
	date_dim
WHERE
	store_returns.sr_returned_date_sk = date_dim.d_date_sk
	AND store_returns.sr_item_sk = item.i_item_sk
	AND (((cast(date_dim.d_date as string) = '2000-06-30') OR (cast(date_dim.d_date as string) = '2000-09-27') OR (cast(date_dim.d_date as string) = '2000-11-17')))