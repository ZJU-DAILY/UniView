CREATE MATERIALIZED VIEW IF NOT EXISTS mv363
PARTITIONED BY (cr_returned_date_sk)
AS
SELECT
	catalog_returns.cr_item_sk,
	catalog_returns.cr_return_quantity,
	catalog_returns.cr_store_credit,
	call_center.cc_name,
	catalog_returns.cr_return_amount,
	catalog_returns.cr_return_amt_inc_tax,
	catalog_returns.cr_reversed_charge,
	catalog_returns.cr_call_center_sk,
	catalog_returns.cr_refunded_cash,
	call_center.cc_manager,
	catalog_returns.cr_returning_customer_sk,
	catalog_returns.cr_order_number,
	catalog_returns.cr_returned_date_sk,
	catalog_returns.cr_catalog_page_sk,
	call_center.cc_call_center_id,
	call_center.cc_county,
	catalog_returns.cr_returning_addr_sk,
	catalog_returns.cr_net_loss,
	call_center.cc_call_center_sk
FROM
	call_center,
	catalog_returns
WHERE
	call_center.cc_call_center_sk = catalog_returns.cr_call_center_sk
DISTRIBUTE BY cr_returned_date_sk;