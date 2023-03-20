SELECT
	date_dim.d_dow,
	catalog_returns.cr_returning_customer_sk,
	call_center.cc_name,
	call_center.cc_county,
	catalog_returns.cr_return_amount,
	date_dim.d_quarter_name,
	date_dim.d_month_seq,
	date_dim.d_day_name,
	date_dim.d_week_seq,
	date_dim.d_qoy,
	catalog_returns.cr_returned_date_sk,
	call_center.cc_manager,
	date_dim.d_dom,
	catalog_returns.cr_item_sk,
	catalog_returns.cr_call_center_sk,
	catalog_returns.cr_order_number,
	catalog_returns.cr_return_amt_inc_tax,
	catalog_returns.cr_refunded_cash,
	date_dim.d_date_sk,
	catalog_returns.cr_net_loss,
	catalog_returns.cr_reversed_charge,
	call_center.cc_call_center_sk,
	catalog_returns.cr_return_quantity,
	date_dim.d_moy,
	date_dim.d_date,
	catalog_returns.cr_catalog_page_sk,
	catalog_returns.cr_returning_addr_sk,
	call_center.cc_call_center_id,
	catalog_returns.cr_store_credit,
	date_dim.d_year
FROM
	call_center,
	catalog_returns,
	date_dim
WHERE
	catalog_returns.cr_returned_date_sk = date_dim.d_date_sk
	AND call_center.cc_call_center_sk = catalog_returns.cr_call_center_sk
	AND date_dim.d_year = 1998
	AND date_dim.d_moy = 11