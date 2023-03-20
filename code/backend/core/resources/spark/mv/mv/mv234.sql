CREATE MATERIALIZED VIEW IF NOT EXISTS mv234
PARTITIONED BY (ws_sold_date_sk)
AS
SELECT
	warehouse.w_city,
	date_dim.d_month_seq,
	web_sales.ws_ship_date_sk,
	web_sales.ws_ship_mode_sk,
	ship_mode.sm_ship_mode_sk,
	date_dim.d_date_sk,
	web_sales.ws_order_number,
	date_dim.d_qoy,
	ship_mode.sm_carrier,
	web_sales.ws_promo_sk,
	web_sales.ws_ship_hdemo_sk,
	web_site.web_name,
	warehouse.w_warehouse_sq_ft,
	date_dim.d_date,
	web_sales.ws_web_site_sk,
	warehouse.w_county,
	web_sales.ws_sold_time_sk,
	ship_mode.sm_type,
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
	date_dim.d_year,
	web_sales.ws_net_paid,
	web_sales.ws_ext_ship_cost,
	web_site.web_site_id,
	web_sales.ws_ship_customer_sk,
	web_sales.ws_ext_discount_amt,
	web_sales.ws_ext_wholesale_cost,
	date_dim.d_quarter_name,
	web_sales.ws_wholesale_cost,
	web_sales.ws_list_price,
	web_sales.ws_sold_date_sk,
	web_site.web_site_sk,
	web_site.web_company_name,
	web_sales.ws_ext_sales_price,
	warehouse.w_warehouse_sk,
	web_sales.ws_ship_addr_sk,
	date_dim.d_day_name,
	date_dim.d_dom,
	web_sales.ws_bill_customer_sk,
	web_sales.ws_net_profit,
	warehouse.w_state,
	date_dim.d_dow
FROM
	date_dim,
	web_sales,
	web_site,
	warehouse,
	ship_mode
WHERE
	web_sales.ws_ship_mode_sk = ship_mode.sm_ship_mode_sk
	AND web_sales.ws_ship_date_sk = date_dim.d_date_sk
	AND web_sales.ws_warehouse_sk = warehouse.w_warehouse_sk
	AND web_sales.ws_web_site_sk = web_site.web_site_sk
	AND (((date_dim.d_month_seq >= 1200) AND (date_dim.d_month_seq <= 1211)))
DISTRIBUTE BY ws_sold_date_sk;