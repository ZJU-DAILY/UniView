SELECT
	date_dim.d_dow,
	store.s_zip,
	store_returns.sr_returned_date_sk,
	store_returns.sr_return_amt,
	date_dim.d_quarter_name,
	date_dim.d_day_name,
	store.s_street_type,
	store_returns.sr_net_loss,
	date_dim.d_qoy,
	store.s_county,
	date_dim.d_dom,
	store_returns.sr_cdemo_sk,
	store.s_street_name,
	store.s_gmt_offset,
	store.s_store_name,
	store_returns.sr_customer_sk,
	store.s_store_sk,
	date_dim.d_week_seq,
	date_dim.d_year,
	store.s_market_id,
	store_returns.sr_return_quantity,
	date_dim.d_month_seq,
	store_returns.sr_item_sk,
	store.s_number_employees,
	store.s_company_id,
	store.s_store_id,
	store.s_company_name,
	store.s_state,
	store.s_suite_number,
	store_returns.sr_reason_sk,
	store_returns.sr_store_sk,
	store_returns.sr_ticket_number,
	date_dim.d_moy,
	date_dim.d_date,
	store.s_city,
	date_dim.d_date_sk,
	store.s_street_number
FROM
	store_returns,
	date_dim,
	store
WHERE
	store_returns.sr_returned_date_sk = date_dim.d_date_sk
	AND store_returns.sr_store_sk = store.s_store_sk
	AND date_dim.d_year = 2000
	AND store.s_state = 'TN'