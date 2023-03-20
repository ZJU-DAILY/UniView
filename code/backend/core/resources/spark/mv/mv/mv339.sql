CREATE MATERIALIZED VIEW IF NOT EXISTS mv339
PARTITIONED BY (wr_returned_date_sk)
AS
SELECT
	web_returns.wr_returning_customer_sk,
	web_returns.wr_fee,
	web_returns.wr_refunded_cash,
	item.i_brand,
	item.i_manufact,
	item.i_units,
	web_returns.wr_item_sk,
	web_returns.wr_return_amt,
	item.i_brand_id,
	item.i_manufact_id,
	web_returns.wr_net_loss,
	web_returns.wr_web_page_sk,
	web_returns.wr_refunded_cdemo_sk,
	item.i_item_sk,
	web_returns.wr_reason_sk,
	item.i_current_price,
	web_returns.wr_refunded_addr_sk,
	item.i_product_name,
	item.i_wholesale_cost,
	item.i_manager_id,
	item.i_class_id,
	item.i_item_desc,
	item.i_color,
	item.i_class,
	web_returns.wr_returned_date_sk,
	web_returns.wr_returning_cdemo_sk,
	web_returns.wr_order_number,
	item.i_category,
	item.i_size,
	item.i_category_id,
	item.i_item_id,
	web_returns.wr_return_quantity,
	web_returns.wr_returning_addr_sk
FROM
	web_returns,
	item
WHERE
	web_returns.wr_item_sk = item.i_item_sk
DISTRIBUTE BY wr_returned_date_sk;