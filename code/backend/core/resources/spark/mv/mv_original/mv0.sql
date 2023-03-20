SELECT
	store_returns.sr_cdemo_sk,
	date_dim.d_month_seq,
	store_returns.sr_customer_sk,
	date_dim.d_date_sk,
	date_dim.d_qoy,
	date_dim.d_quarter_name,
	date_dim.d_date,
	store_returns.sr_net_loss,
	store_returns.sr_ticket_number,
	store_returns.sr_returned_date_sk,
	store_returns.sr_item_sk,
	store_returns.sr_return_amt,
	date_dim.d_day_name,
	date_dim.d_dom,
	store_returns.sr_reason_sk,
	date_dim.d_moy,
	store_returns.sr_store_sk,
	date_dim.d_week_seq,
	store_returns.sr_return_quantity,
	date_dim.d_dow,
	date_dim.d_year
FROM
	date_dim,
	store_returns
WHERE
	date_dim.d_year = 2000
	AND store_returns.sr_returned_date_sk = date_dim.d_date_sk