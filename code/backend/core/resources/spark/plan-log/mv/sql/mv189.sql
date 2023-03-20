SELECT
	date_dim.d_dow,
	store_returns.sr_returned_date_sk,
	store_sales.ss_promo_sk,
	store_returns.sr_return_amt,
	date_dim.d_quarter_name,
	store_sales.ss_ext_discount_amt,
	date_dim.d_day_name,
	store_sales.ss_store_sk,
	store_sales.ss_item_sk,
	store_sales.ss_customer_sk,
	store_returns.sr_net_loss,
	date_dim.d_qoy,
	date_dim.d_dom,
	store_returns.sr_cdemo_sk,
	store_sales.ss_sold_date_sk,
	store_sales.ss_cdemo_sk,
	store_sales.ss_coupon_amt,
	store_sales.ss_addr_sk,
	store_sales.ss_hdemo_sk,
	store_returns.sr_customer_sk,
	date_dim.d_week_seq,
	date_dim.d_year,
	store_sales.ss_ext_sales_price,
	store_sales.ss_ticket_number,
	store_sales.ss_list_price,
	store_returns.sr_return_quantity,
	date_dim.d_month_seq,
	store_returns.sr_item_sk,
	store_sales.ss_quantity,
	store_sales.ss_ext_tax,
	store_sales.ss_ext_wholesale_cost,
	store_returns.sr_reason_sk,
	store_sales.ss_ext_list_price,
	store_returns.sr_store_sk,
	store_returns.sr_ticket_number,
	date_dim.d_moy,
	store_sales.ss_sales_price,
	store_sales.ss_sold_time_sk,
	date_dim.d_date,
	store_sales.ss_net_profit,
	store_sales.ss_wholesale_cost,
	date_dim.d_date_sk,
	store_sales.ss_net_paid
FROM
	store_sales,
	store_returns,
	date_dim
WHERE
	store_sales.ss_ticket_number = store_returns.sr_ticket_number
	AND store_sales.ss_item_sk = store_returns.sr_item_sk
	AND store_sales.ss_quantity > 0
	AND store_sales.ss_sold_date_sk = date_dim.d_date_sk
	AND date_dim.d_moy = 12
	AND date_dim.d_year = 2001