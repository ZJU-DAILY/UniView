SELECT
	customer_address.ca_location_type,
	date_dim.d_month_seq,
	web_sales.ws_ship_date_sk,
	web_sales.ws_ship_mode_sk,
	customer_address.ca_street_type,
	date_dim.d_date_sk,
	web_sales.ws_order_number,
	date_dim.d_qoy,
	web_sales.ws_promo_sk,
	customer_address.ca_county,
	web_sales.ws_ship_hdemo_sk,
	customer_address.ca_city,
	date_dim.d_date,
	web_sales.ws_web_site_sk,
	web_sales.ws_sold_time_sk,
	customer_address.ca_street_name,
	web_sales.ws_sales_price,
	web_sales.ws_ext_list_price,
	customer_address.ca_zip,
	web_sales.ws_warehouse_sk,
	web_sales.ws_quantity,
	date_dim.d_moy,
	web_sales.ws_bill_addr_sk,
	web_sales.ws_web_page_sk,
	date_dim.d_week_seq,
	web_sales.ws_ext_ship_cost,
	date_dim.d_year,
	customer_address.ca_address_sk,
	web_sales.ws_net_paid,
	web_sales.ws_ship_customer_sk,
	web_sales.ws_ext_discount_amt,
	web_sales.ws_ext_wholesale_cost,
	date_dim.d_quarter_name,
	web_sales.ws_wholesale_cost,
	customer_address.ca_street_number,
	customer_address.ca_state,
	web_sales.ws_list_price,
	web_sales.ws_sold_date_sk,
	customer_address.ca_gmt_offset,
	web_sales.ws_ship_addr_sk,
	web_sales.ws_ext_sales_price,
	customer_address.ca_country,
	customer_address.ca_suite_number,
	date_dim.d_day_name,
	date_dim.d_dom,
	web_sales.ws_bill_customer_sk,
	web_sales.ws_net_profit,
	web_sales.ws_item_sk,
	date_dim.d_dow
FROM
	web_sales SEMI JOIN web_returns ON (web_sales.ws_order_number = web_returns.wr_order_number)	Join date_dim
	Join customer_address
WHERE
	web_sales.ws_ship_date_sk = date_dim.d_date_sk
	AND web_sales.ws_ship_addr_sk = customer_address.ca_address_sk
	AND (((date_dim.d_date >= cast('1970-01-01' as date) + interval 10623 days) AND (date_dim.d_date <= cast('1970-01-01' as date) + interval 10683 days)))
	AND customer_address.ca_state = 'IL'