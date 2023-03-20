CREATE MATERIALIZED VIEW IF NOT EXISTS mv336
PARTITIONED BY (cr_returned_date_sk)
AS
SELECT
	item.i_brand,
	catalog_returns.cr_item_sk,
	item.i_manufact,
	item.i_units,
	catalog_returns.cr_return_quantity,
	catalog_returns.cr_store_credit,
	item.i_brand_id,
	catalog_returns.cr_return_amount,
	catalog_returns.cr_return_amt_inc_tax,
	catalog_returns.cr_reversed_charge,
	item.i_item_sk,
	item.i_product_name,
	catalog_returns.cr_call_center_sk,
	item.i_wholesale_cost,
	catalog_returns.cr_refunded_cash,
	item.i_class_id,
	item.i_manager_id,
	item.i_item_desc,
	item.i_color,
	item.i_class,
	catalog_returns.cr_returning_customer_sk,
	catalog_returns.cr_order_number,
	catalog_returns.cr_returned_date_sk,
	catalog_returns.cr_catalog_page_sk,
	item.i_category,
	catalog_returns.cr_returning_addr_sk,
	item.i_size,
	catalog_returns.cr_net_loss,
	item.i_category_id,
	item.i_item_id,
	item.i_current_price,
	item.i_manufact_id
FROM
	catalog_returns,
	item
WHERE
	catalog_returns.cr_item_sk = item.i_item_sk
DISTRIBUTE BY cr_returned_date_sk;