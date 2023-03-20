SELECT
	store_sales.ss_hdemo_sk,
	store_sales.ss_ext_sales_price,
	store_sales.ss_ticket_number,
	store_sales.ss_cdemo_sk,
	store_sales.ss_list_price,
	store_returns.sr_ticket_number,
	store_sales.ss_store_sk,
	store_sales.ss_ext_wholesale_cost,
	reason.r_reason_desc,
	store_returns.sr_reason_sk,
	store_sales.ss_ext_list_price,
	store_sales.ss_customer_sk,
	store_returns.sr_return_quantity,
	store_returns.sr_cdemo_sk,
	store_sales.ss_net_profit,
	store_sales.ss_sold_date_sk,
	store_returns.sr_customer_sk,
	store_sales.ss_addr_sk,
	store_sales.ss_sales_price,
	store_sales.ss_item_sk,
	store_sales.ss_coupon_amt,
	store_sales.ss_sold_time_sk,
	store_returns.sr_net_loss,
	store_returns.sr_returned_date_sk,
	store_sales.ss_ext_discount_amt,
	store_sales.ss_promo_sk,
	store_sales.ss_wholesale_cost,
	store_returns.sr_item_sk,
	store_returns.sr_return_amt,
	reason.r_reason_sk,
	store_sales.ss_ext_tax,
	store_returns.sr_store_sk,
	store_sales.ss_net_paid,
	store_sales.ss_quantity
FROM
	store_sales,
	store_returns,
	reason
WHERE
	reason.r_reason_desc = 'reason 28'
	AND store_returns.sr_reason_sk = reason.r_reason_sk
	AND store_sales.ss_item_sk = store_returns.sr_item_sk
	AND store_sales.ss_ticket_number = store_returns.sr_ticket_number