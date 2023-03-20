SELECT
	date_dim.d_dow,
	store.s_zip,
	store_sales.ss_promo_sk,
	promotion.p_channel_dmail,
	date_dim.d_quarter_name,
	store_sales.ss_ext_discount_amt,
	date_dim.d_day_name,
	store_sales.ss_store_sk,
	store.s_street_type,
	store_sales.ss_item_sk,
	store_sales.ss_customer_sk,
	date_dim.d_qoy,
	store.s_county,
	date_dim.d_dom,
	store_sales.ss_sold_date_sk,
	store.s_street_name,
	store_sales.ss_cdemo_sk,
	store.s_gmt_offset,
	store_sales.ss_coupon_amt,
	store_sales.ss_addr_sk,
	store_sales.ss_hdemo_sk,
	store.s_store_name,
	store.s_store_sk,
	store_sales.ss_net_paid,
	promotion.p_channel_tv,
	date_dim.d_week_seq,
	date_dim.d_year,
	promotion.p_promo_sk,
	store_sales.ss_ext_sales_price,
	store.s_market_id,
	store_sales.ss_list_price,
	store_sales.ss_ticket_number,
	date_dim.d_month_seq,
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
	date_dim.d_moy,
	store_sales.ss_sold_time_sk,
	date_dim.d_date,
	store_sales.ss_net_profit,
	store.s_city,
	store_sales.ss_wholesale_cost,
	date_dim.d_date_sk,
	store.s_street_number
FROM
	date_dim,
	store_sales,
	store,
	promotion
WHERE
	promotion.p_channel_email = 'Y'
	AND store_sales.ss_promo_sk = promotion.p_promo_sk
	AND promotion.p_channel_tv = 'Y'
	AND promotion.p_channel_dmail = 'Y'
	AND store_sales.ss_sold_date_sk = date_dim.d_date_sk
	AND store_sales.ss_store_sk = store.s_store_sk
	AND date_dim.d_moy = 11
	AND date_dim.d_year = 1998