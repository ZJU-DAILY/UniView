SELECT
	store.s_zip,
	promotion.p_channel_dmail,
	store_sales.ss_promo_sk,
	store_sales.ss_ext_discount_amt,
	store_sales.ss_store_sk,
	store.s_street_type,
	store_sales.ss_item_sk,
	store_sales.ss_customer_sk,
	store.s_county,
	store_sales.ss_sold_date_sk,
	store.s_street_name,
	store_sales.ss_cdemo_sk,
	store.s_gmt_offset,
	store_sales.ss_coupon_amt,
	store_sales.ss_addr_sk,
	store_sales.ss_hdemo_sk,
	store.s_store_name,
	store.s_store_sk,
	promotion.p_channel_tv,
	promotion.p_promo_sk,
	store_sales.ss_ext_sales_price,
	store.s_market_id,
	store_sales.ss_list_price,
	store_sales.ss_ticket_number,
	store_sales.ss_quantity,
	store_sales.ss_ext_tax,
	store.s_number_employees,
	store.s_company_id,
	store.s_store_id,
	promotion.p_channel_event,
	store.s_company_name,
	store.s_state,
	store_sales.ss_ext_wholesale_cost,
	store.s_suite_number,
	promotion.p_channel_email,
	store_sales.ss_ext_list_price,
	store_sales.ss_sales_price,
	store_sales.ss_sold_time_sk,
	store.s_city,
	store_sales.ss_net_profit,
	store.s_street_number,
	store_sales.ss_wholesale_cost,
	store_sales.ss_net_paid
FROM
	store_sales,
	promotion,
	store
WHERE
	promotion.p_channel_tv = 'Y'
	AND promotion.p_channel_email = 'Y'
	AND store_sales.ss_promo_sk = promotion.p_promo_sk
	AND promotion.p_channel_dmail = 'Y'
	AND store_sales.ss_store_sk = store.s_store_sk