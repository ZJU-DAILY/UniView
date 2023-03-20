CREATE MATERIALIZED VIEW IF NOT EXISTS mv0 AS
SELECT
	date_dim.d_dow,
	store_returns.sr_returned_date_sk,
	store_returns.sr_return_quantity,
	store_returns.sr_return_amt,
	date_dim.d_quarter_name,
	date_dim.d_month_seq,
	store_returns.sr_item_sk,
	date_dim.d_day_name,
	store_returns.sr_net_loss,
	date_dim.d_qoy,
	date_dim.d_dom,
	store_returns.sr_reason_sk,
	store_returns.sr_cdemo_sk,
	store_returns.sr_store_sk,
	date_dim.d_date_sk,
	store_returns.sr_ticket_number,
	date_dim.d_moy,
	date_dim.d_date,
	store_returns.sr_customer_sk,
	date_dim.d_week_seq,
	date_dim.d_year
FROM
	store_returns,
	date_dim
WHERE
	store_returns.sr_returned_date_sk = date_dim.d_date_sk
	AND date_dim.d_year = 2000