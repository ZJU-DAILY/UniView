CREATE MATERIALIZED VIEW IF NOT EXISTS mv329
PARTITIONED BY (cr_returned_date_sk)
AS
SELECT
	catalog_returns.cr_item_sk,
	date_dim.d_month_seq,
	catalog_returns.cr_return_quantity,
	catalog_returns.cr_store_credit,
	date_dim.d_date_sk,
	date_dim.d_qoy,
	date_dim.d_quarter_name,
	catalog_returns.cr_return_amount,
	catalog_returns.cr_return_amt_inc_tax,
	catalog_returns.cr_reversed_charge,
	catalog_returns.cr_call_center_sk,
	date_dim.d_date,
	catalog_returns.cr_refunded_cash,
	catalog_returns.cr_returning_customer_sk,
	catalog_returns.cr_order_number,
	catalog_returns.cr_returned_date_sk,
	catalog_returns.cr_catalog_page_sk,
	catalog_returns.cr_returning_addr_sk,
	date_dim.d_day_name,
	catalog_returns.cr_net_loss,
	date_dim.d_dom,
	date_dim.d_moy,
	date_dim.d_week_seq,
	date_dim.d_dow,
	date_dim.d_year
FROM
	date_dim,
	catalog_returns
WHERE
	date_dim.d_year = 2000
	AND catalog_returns.cr_returned_date_sk = date_dim.d_date_sk
DISTRIBUTE BY cr_returned_date_sk;