SELECT
	date_dim.d_month_seq,
	store.s_suite_number,
	date_dim.d_date_sk,
	date_dim.d_qoy,
	store.s_store_name,
	store.s_zip,
	date_dim.d_date,
	store_returns.sr_ticket_number,
	store.s_store_id,
	store.s_company_name,
	store.s_street_name,
	store.s_city,
	store_returns.sr_reason_sk,
	store.s_market_id,
	date_dim.d_moy,
	date_dim.d_week_seq,
	store_returns.sr_return_quantity,
	store.s_gmt_offset,
	date_dim.d_year,
	store_returns.sr_cdemo_sk,
	store.s_state,
	store_returns.sr_customer_sk,
	date_dim.d_quarter_name,
	store.s_company_id,
	store_returns.sr_net_loss,
	store.s_store_sk,
	store_returns.sr_returned_date_sk,
	store_returns.sr_item_sk,
	store.s_number_employees,
	store_returns.sr_return_amt,
	date_dim.d_day_name,
	store.s_street_number,
	date_dim.d_dom,
	store.s_county,
	store_returns.sr_store_sk,
	store.s_street_type,
	date_dim.d_dow
FROM
	date_dim,
	store_returns,
	store
WHERE
	store.s_state = 'TN'
	AND store_returns.sr_store_sk = store.s_store_sk
	AND date_dim.d_year = 2000
	AND store_returns.sr_returned_date_sk = date_dim.d_date_sk