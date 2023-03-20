CREATE MATERIALIZED VIEW IF NOT EXISTS mv382
PARTITIONED BY (ss_sold_date_sk)
AS
SELECT
	store_sales.ss_net_profit,
	store_sales.ss_hdemo_sk,
	store_sales.ss_ext_sales_price,
	household_demographics.hd_dep_count,
	store_sales.ss_sold_date_sk,
	household_demographics.hd_demo_sk,
	store_sales.ss_addr_sk,
	household_demographics.hd_vehicle_count,
	store_sales.ss_sales_price,
	store_sales.ss_item_sk,
	store_sales.ss_coupon_amt,
	time_dim.t_time_sk,
	household_demographics.hd_buy_potential,
	store_sales.ss_ticket_number,
	store_sales.ss_sold_time_sk,
	time_dim.t_time,
	store_sales.ss_cdemo_sk,
	time_dim.t_meal_time,
	store_sales.ss_list_price,
	household_demographics.hd_income_band_sk,
	store_sales.ss_store_sk,
	store_sales.ss_ext_discount_amt,
	store_sales.ss_promo_sk,
	store_sales.ss_wholesale_cost,
	store_sales.ss_ext_wholesale_cost,
	time_dim.t_minute,
	time_dim.t_hour,
	store_sales.ss_ext_tax,
	store_sales.ss_customer_sk,
	store_sales.ss_ext_list_price,
	store_sales.ss_net_paid,
	store_sales.ss_quantity
FROM
	store_sales,
	time_dim,
	household_demographics
WHERE
	household_demographics.hd_dep_count = 7
	AND store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
	AND store_sales.ss_sold_time_sk = time_dim.t_time_sk
	AND time_dim.t_hour = 20
	AND time_dim.t_minute >= 30
DISTRIBUTE BY ss_sold_date_sk;