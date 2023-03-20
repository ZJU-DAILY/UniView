SELECT
	store.s_suite_number,
	store_sales.ss_hdemo_sk,
	store_sales.ss_ext_sales_price,
	household_demographics.hd_vehicle_count,
	store.s_store_name,
	time_dim.t_time_sk,
	household_demographics.hd_buy_potential,
	store_sales.ss_ticket_number,
	store.s_zip,
	store_sales.ss_cdemo_sk,
	store_sales.ss_list_price,
	store.s_store_id,
	store_sales.ss_store_sk,
	store_sales.ss_ext_wholesale_cost,
	store.s_company_name,
	store.s_street_name,
	store.s_city,
	store.s_market_id,
	store_sales.ss_net_paid,
	store_sales.ss_ext_list_price,
	store_sales.ss_customer_sk,
	household_demographics.hd_dep_count,
	store.s_gmt_offset,
	store.s_state,
	store_sales.ss_net_profit,
	store_sales.ss_sold_date_sk,
	household_demographics.hd_demo_sk,
	store_sales.ss_addr_sk,
	store_sales.ss_sales_price,
	store_sales.ss_item_sk,
	store_sales.ss_coupon_amt,
	store.s_company_id,
	store_sales.ss_sold_time_sk,
	time_dim.t_meal_time,
	store.s_store_sk,
	household_demographics.hd_income_band_sk,
	store_sales.ss_ext_discount_amt,
	store_sales.ss_promo_sk,
	store_sales.ss_wholesale_cost,
	time_dim.t_minute,
	store.s_number_employees,
	store.s_street_number,
	store.s_county,
	time_dim.t_hour,
	store_sales.ss_ext_tax,
	store.s_street_type,
	time_dim.t_time,
	store_sales.ss_quantity
FROM
	store_sales,
	store,
	time_dim,
	household_demographics
WHERE
	store_sales.ss_store_sk = store.s_store_sk
	AND store_sales.ss_sold_time_sk = time_dim.t_time_sk
	AND store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
	AND store.s_store_name = 'ese'
	AND household_demographics.hd_dep_count = 7
	AND time_dim.t_hour = 20
	AND time_dim.t_minute >= 30