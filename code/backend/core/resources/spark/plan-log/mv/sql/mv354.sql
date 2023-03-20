SELECT
	store_sales.ss_ext_sales_price,
	store_sales.ss_ticket_number,
	store_sales.ss_promo_sk,
	store_sales.ss_list_price,
	store_sales.ss_ext_discount_amt,
	store_sales.ss_store_sk,
	store_sales.ss_quantity,
	store_sales.ss_ext_tax,
	store_sales.ss_wholesale_cost,
	time_dim.t_time,
	store_sales.ss_item_sk,
	store_sales.ss_customer_sk,
	store_sales.ss_ext_wholesale_cost,
	time_dim.t_time_sk,
	store_sales.ss_ext_list_price,
	household_demographics.hd_buy_potential,
	time_dim.t_hour,
	store_sales.ss_sold_date_sk,
	household_demographics.hd_vehicle_count,
	store_sales.ss_cdemo_sk,
	store_sales.ss_coupon_amt,
	time_dim.t_minute,
	store_sales.ss_sales_price,
	store_sales.ss_sold_time_sk,
	store_sales.ss_addr_sk,
	time_dim.t_meal_time,
	store_sales.ss_net_profit,
	household_demographics.hd_dep_count,
	store_sales.ss_hdemo_sk,
	household_demographics.hd_income_band_sk,
	household_demographics.hd_demo_sk,
	store_sales.ss_net_paid
FROM
	store_sales,
	household_demographics,
	time_dim
WHERE
	household_demographics.hd_vehicle_count <= 6
	AND (((household_demographics.hd_dep_count = 4) OR (household_demographics.hd_dep_count = 2) OR (household_demographics.hd_dep_count = 0)))
	AND store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
	AND (time_dim.t_hour = 8 OR time_dim.t_hour = 9 OR time_dim.t_hour = 10 OR time_dim.t_hour = 11 OR time_dim.t_hour = 12)
	AND (time_dim.t_minute >= 30 OR time_dim.t_minute < 30)
	AND store_sales.ss_sold_time_sk = time_dim.t_time_sk