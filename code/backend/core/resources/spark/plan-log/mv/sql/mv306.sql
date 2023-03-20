SELECT
	date_dim.d_dow,
	web_page.wp_web_page_sk,
	web_returns.wr_fee,
	date_dim.d_quarter_name,
	date_dim.d_month_seq,
	web_returns.wr_item_sk,
	date_dim.d_day_name,
	web_returns.wr_order_number,
	web_page.wp_char_count,
	web_returns.wr_returning_customer_sk,
	date_dim.d_qoy,
	web_returns.wr_returned_date_sk,
	date_dim.d_dom,
	web_returns.wr_net_loss,
	web_returns.wr_refunded_cash,
	web_returns.wr_web_page_sk,
	web_returns.wr_refunded_addr_sk,
	web_returns.wr_return_amt,
	date_dim.d_date_sk,
	web_returns.wr_return_quantity,
	web_returns.wr_returning_cdemo_sk,
	date_dim.d_moy,
	date_dim.d_date,
	web_returns.wr_returning_addr_sk,
	date_dim.d_week_seq,
	web_returns.wr_refunded_cdemo_sk,
	web_returns.wr_reason_sk,
	date_dim.d_year
FROM
	web_returns,
	date_dim,
	web_page
WHERE
	web_returns.wr_returned_date_sk = date_dim.d_date_sk
	AND web_returns.wr_web_page_sk = web_page.wp_web_page_sk
	AND (((date_dim.d_date >= cast('1970-01-01' as date) + interval 11172 days) AND (date_dim.d_date <= cast('1970-01-01' as date) + interval 11202 days)))