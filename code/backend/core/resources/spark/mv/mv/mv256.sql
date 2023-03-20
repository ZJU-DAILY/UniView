CREATE MATERIALIZED VIEW IF NOT EXISTS mv256
PARTITIONED BY (ws_sold_date_sk)
AS
SELECT
	warehouse.w_city,
	date_dim.d_month_seq,
	web_sales.ws_ship_date_sk,
	web_sales.ws_ship_mode_sk,
	date_dim.d_date_sk,
	web_sales.ws_order_number,
	date_dim.d_qoy,
	web_sales.ws_promo_sk,
	time_dim.t_time_sk,
	web_sales.ws_ship_hdemo_sk,
	warehouse.w_warehouse_sq_ft,
	date_dim.d_date,
	web_sales.ws_web_site_sk,
	warehouse.w_county,
	web_sales.ws_sold_time_sk,
	web_sales.ws_sales_price,
	warehouse.w_warehouse_name,
	web_sales.ws_ext_list_price,
	warehouse.w_country,
	web_sales.ws_warehouse_sk,
	web_sales.ws_quantity,
	date_dim.d_moy,
	web_sales.ws_bill_addr_sk,
	web_sales.ws_web_page_sk,
	date_dim.d_week_seq,
	web_sales.ws_item_sk,
	web_sales.ws_net_paid,
	date_dim.d_year,
	web_sales.ws_ext_ship_cost,
	web_sales.ws_ship_customer_sk,
	web_sales.ws_ext_discount_amt,
	web_sales.ws_ext_wholesale_cost,
	date_dim.d_quarter_name,
	web_sales.ws_wholesale_cost,
	web_sales.ws_net_profit,
	web_sales.ws_list_price,
	time_dim.t_meal_time,
	web_sales.ws_sold_date_sk,
	web_sales.ws_ext_sales_price,
	warehouse.w_warehouse_sk,
	web_sales.ws_ship_addr_sk,
	time_dim.t_minute,
	date_dim.d_day_name,
	date_dim.d_dom,
	time_dim.t_hour,
	web_sales.ws_bill_customer_sk,
	time_dim.t_time,
	warehouse.w_state,
	date_dim.d_dow
FROM
	date_dim,
	web_sales,
	time_dim,
	warehouse
WHERE
	(((time_dim.t_time >= 30838) AND (time_dim.t_time <= 59638)))
	AND web_sales.ws_sold_time_sk = time_dim.t_time_sk
	AND web_sales.ws_sold_date_sk = date_dim.d_date_sk
	AND web_sales.ws_warehouse_sk = warehouse.w_warehouse_sk
	AND date_dim.d_year = 2001
DISTRIBUTE BY ws_sold_date_sk;