SELECT
	date_dim.d_dow,
	store.s_zip,
	store_sales.ss_promo_sk,
	date_dim.d_quarter_name,
	store_sales.ss_ext_discount_amt,
	date_dim.d_day_name,
	store_sales.ss_store_sk,
	store.s_street_type,
	store_sales.ss_customer_sk,
	store_sales.ss_item_sk,
	date_dim.d_qoy,
	store.s_county,
	date_dim.d_dom,
	store_sales.ss_sold_date_sk,
	store.s_street_name,
	household_demographics.hd_vehicle_count,
	store_sales.ss_cdemo_sk,
	store_sales.ss_coupon_amt,
	store.s_gmt_offset,
	store_sales.ss_addr_sk,
	store.s_store_name,
	store.s_store_sk,
	household_demographics.hd_dep_count,
	household_demographics.hd_income_band_sk,
	date_dim.d_week_seq,
	date_dim.d_year,
	household_demographics.hd_demo_sk,
	store_sales.ss_ext_sales_price,
	store.s_market_id,
	store_sales.ss_list_price,
	store_sales.ss_ticket_number,
	date_dim.d_month_seq,
	store_sales.ss_quantity,
	store_sales.ss_ext_tax,
	store_sales.ss_wholesale_cost,
	store.s_number_employees,
	store.s_company_id,
	store.s_store_id,
	store.s_company_name,
	store.s_state,
	store_sales.ss_ext_wholesale_cost,
	store.s_suite_number,
	store_sales.ss_ext_list_price,
	household_demographics.hd_buy_potential,
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
	household_demographics,
	date_dim,
	store
WHERE
	store_sales.ss_sold_date_sk = date_dim.d_date_sk
	AND store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
	AND store_sales.ss_store_sk = store.s_store_sk
	AND (((date_dim.d_dow = 6) OR (date_dim.d_dow = 0)))
	AND (((date_dim.d_year = 1999) OR (date_dim.d_year = 2000) OR (date_dim.d_year = 2001)))
	AND household_demographics.hd_vehicle_count = 3
	AND household_demographics.hd_dep_count = 4
	AND (((store.s_city = 'Fairview') OR (store.s_city = 'Midway')))