SELECT
	date_dim.d_dow,
	store.s_zip,
	store_sales.ss_promo_sk,
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
	store_sales.ss_coupon_amt,
	store.s_gmt_offset,
	store_sales.ss_addr_sk,
	store.s_store_name,
	store.s_store_sk,
	date_dim.d_week_seq,
	date_dim.d_year,
	store_sales.ss_ext_sales_price,
	store.s_market_id,
	store_sales.ss_ticket_number,
	store_sales.ss_list_price,
	date_dim.d_month_seq,
	store_sales.ss_ext_tax,
	store_sales.ss_quantity,
	store_sales.ss_wholesale_cost,
	store.s_number_employees,
	store.s_company_id,
	store.s_store_id,
	store.s_company_name,
	store.s_state,
	store_sales.ss_ext_wholesale_cost,
	store.s_suite_number,
	store_sales.ss_ext_list_price,
	store_sales.ss_sales_price,
	store_sales.ss_sold_time_sk,
	date_dim.d_moy,
	store.s_city,
	store_sales.ss_net_profit,
	date_dim.d_date,
	store.s_street_number,
	store_sales.ss_hdemo_sk,
	date_dim.d_date_sk,
	store_sales.ss_net_paid
FROM
	store_sales,
	date_dim,
	store
WHERE
	date_dim.d_dom <= 2
	AND date_dim.d_dom >= 1
	AND store_sales.ss_sold_date_sk = date_dim.d_date_sk
	AND (((date_dim.d_year = 1999) OR (date_dim.d_year = 2000) OR (date_dim.d_year = 2001)))
	AND store_sales.ss_store_sk = store.s_store_sk
	AND (((store.s_city = 'Midway') OR (store.s_city = 'Fairview')))