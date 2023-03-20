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
	store_sales.ss_item_sk,
	store_sales.ss_customer_sk,
	store_sales.ss_ext_wholesale_cost,
	store_sales.ss_ext_list_price,
	household_demographics.hd_buy_potential,
	store_sales.ss_sold_date_sk,
	household_demographics.hd_vehicle_count,
	store_sales.ss_cdemo_sk,
	store_sales.ss_coupon_amt,
	store_sales.ss_sales_price,
	store_sales.ss_sold_time_sk,
	store_sales.ss_addr_sk,
	store_sales.ss_net_profit,
	household_demographics.hd_dep_count,
	store_sales.ss_hdemo_sk,
	household_demographics.hd_income_band_sk,
	household_demographics.hd_demo_sk,
	store_sales.ss_net_paid
FROM
	store_sales,
	household_demographics
WHERE
	household_demographics.hd_vehicle_count <= 6
	AND store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
	AND (((household_demographics.hd_dep_count = 4) OR (household_demographics.hd_dep_count = 2) OR (household_demographics.hd_dep_count = 0)))