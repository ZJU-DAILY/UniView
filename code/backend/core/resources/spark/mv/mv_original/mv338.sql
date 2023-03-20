SELECT
	item.i_brand,
	catalog_returns.cr_item_sk,
	date_dim.d_month_seq,
	item.i_brand_id,
	date_dim.d_date_sk,
	date_dim.d_qoy,
	catalog_returns.cr_reversed_charge,
	date_dim.d_date,
	store_returns.sr_ticket_number,
	item.i_manager_id,
	item.i_class_id,
	item.i_item_desc,
	item.i_class,
	catalog_returns.cr_order_number,
	item.i_category,
	catalog_returns.cr_returning_addr_sk,
	item.i_size,
	catalog_returns.cr_net_loss,
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
	catalog_returns.cr_return_quantity,
	store_returns.sr_customer_sk,
	catalog_returns.cr_store_credit,
	date_dim.d_quarter_name,
	catalog_returns.cr_return_amount,
	catalog_returns.cr_return_amt_inc_tax,
	item.i_item_sk,
	item.i_product_name,
	catalog_returns.cr_call_center_sk,
	item.i_wholesale_cost,
	store_returns.sr_net_loss,
	store_returns.sr_returned_date_sk,
	catalog_returns.cr_refunded_cash,
	item.i_color,
	catalog_returns.cr_returning_customer_sk,
	store_returns.sr_item_sk,
	catalog_returns.cr_returned_date_sk,
	catalog_returns.cr_catalog_page_sk,
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
	catalog_returns,
	item
WHERE
	catalog_returns.cr_returned_date_sk = date_dim.d_date_sk
	AND catalog_returns.cr_item_sk = item.i_item_sk
	AND store_returns.sr_returned_date_sk = date_dim.d_date_sk
	AND (((cast(date_dim.d_date as string) = '2000-06-30') OR (cast(date_dim.d_date as string) = '2000-09-27') OR (cast(date_dim.d_date as string) = '2000-11-17')))
	AND store_returns.sr_item_sk = item.i_item_sk