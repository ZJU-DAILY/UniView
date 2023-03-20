CREATE MATERIALIZED VIEW IF NOT EXISTS mv364
PARTITIONED BY (cr_returned_date_sk)
AS
SELECT
	date_dim.d_dow,
	date_dim.d_month_seq,
	catalog_returns.cr_item_sk,
	catalog_returns.cr_return_quantity,
	catalog_returns.cr_store_credit,
	date_dim.d_date_sk,
	call_center.cc_name,
	date_dim.d_qoy,
	date_dim.d_quarter_name,
	catalog_returns.cr_return_amount,
	catalog_returns.cr_return_amt_inc_tax,
	catalog_returns.cr_reversed_charge,
	catalog_returns.cr_call_center_sk,
	date_dim.d_date,
	catalog_returns.cr_refunded_cash,
	call_center.cc_manager,
	catalog_returns.cr_returning_customer_sk,
	catalog_returns.cr_order_number,
	catalog_returns.cr_returned_date_sk,
	catalog_returns.cr_catalog_page_sk,
	call_center.cc_call_center_id,
	call_center.cc_county,
	catalog_returns.cr_returning_addr_sk,
	date_dim.d_day_name,
	catalog_returns.cr_net_loss,
	date_dim.d_dom,
	date_dim.d_moy,
	date_dim.d_week_seq,
	call_center.cc_call_center_sk,
	date_dim.d_year
FROM
	date_dim,
	call_center,
	catalog_returns
WHERE
	catalog_returns.cr_returned_date_sk = date_dim.d_date_sk
	AND call_center.cc_call_center_sk = catalog_returns.cr_call_center_sk
	AND date_dim.d_year = 1998
	AND date_dim.d_moy = 11
DISTRIBUTE BY cr_returned_date_sk;