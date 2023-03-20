SELECT
	web_sales.ws_ship_mode_sk,
	web_page.wp_web_page_sk,
	web_sales.ws_ship_hdemo_sk,
	web_sales.ws_wholesale_cost,
	web_sales.ws_ext_list_price,
	web_page.wp_char_count,
	time_dim.t_time,
	web_sales.ws_list_price,
	time_dim.t_time_sk,
	web_sales.ws_ship_customer_sk,
	web_sales.ws_ship_addr_sk,
	web_sales.ws_order_number,
	web_sales.ws_web_page_sk,
	household_demographics.hd_vehicle_count,
	web_sales.ws_bill_addr_sk,
	web_sales.ws_ext_ship_cost,
	web_sales.ws_warehouse_sk,
	household_demographics.hd_dep_count,
	household_demographics.hd_income_band_sk,
	web_sales.ws_ship_date_sk,
	household_demographics.hd_demo_sk,
	web_sales.ws_bill_customer_sk,
	web_sales.ws_net_profit,
	web_sales.ws_sold_time_sk,
	web_sales.ws_sales_price,
	web_sales.ws_ext_wholesale_cost,
	web_sales.ws_item_sk,
	web_sales.ws_sold_date_sk,
	web_sales.ws_web_site_sk,
	web_sales.ws_net_paid,
	web_sales.ws_promo_sk,
	household_demographics.hd_buy_potential,
	time_dim.t_hour,
	time_dim.t_minute,
	web_sales.ws_quantity,
	time_dim.t_meal_time,
	web_sales.ws_ext_sales_price,
	web_sales.ws_ext_discount_amt
FROM
	household_demographics,
	time_dim,
	web_sales,
	web_page
WHERE
	web_sales.ws_sold_time_sk = time_dim.t_time_sk
	AND web_sales.ws_ship_hdemo_sk = household_demographics.hd_demo_sk
	AND web_sales.ws_web_page_sk = web_page.wp_web_page_sk
	AND household_demographics.hd_dep_count = 6
	AND (((web_page.wp_char_count >= 5000) AND (web_page.wp_char_count <= 5200)))