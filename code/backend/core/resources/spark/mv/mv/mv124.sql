CREATE MATERIALIZED VIEW IF NOT EXISTS mv124
PARTITIONED BY (wr_returned_date_sk)
AS
SELECT
	web_returns.wr_returning_customer_sk,
	web_returns.wr_fee,
	date_dim.d_month_seq,
	web_returns.wr_refunded_cash,
	web_returns.wr_item_sk,
	web_returns.wr_return_amt,
	date_dim.d_date_sk,
	date_dim.d_qoy,
	date_dim.d_quarter_name,
	web_returns.wr_net_loss,
	web_returns.wr_web_page_sk,
	web_returns.wr_refunded_cdemo_sk,
	web_returns.wr_reason_sk,
	web_returns.wr_refunded_addr_sk,
	date_dim.d_date,
	web_returns.wr_returned_date_sk,
	web_returns.wr_returning_cdemo_sk,
	web_returns.wr_order_number,
	date_dim.d_day_name,
	date_dim.d_dom,
	date_dim.d_moy,
	web_returns.wr_return_quantity,
	web_returns.wr_returning_addr_sk,
	date_dim.d_week_seq,
	date_dim.d_dow,
	date_dim.d_year
FROM
	date_dim,
	web_returns
WHERE
	web_returns.wr_returned_date_sk = date_dim.d_date_sk
	AND date_dim.d_year = 2002
DISTRIBUTE BY wr_returned_date_sk;