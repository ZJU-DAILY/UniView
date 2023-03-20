SELECT
	date_dim.d_dow,
	web_sales.ws_ship_mode_sk,
	customer_address.ca_location_type,
	customer_address.ca_city,
	store_sales.ss_promo_sk,
	web_sales.ws_ship_hdemo_sk,
	date_dim.d_quarter_name,
	web_sales.ws_wholesale_cost,
	store_sales.ss_ext_discount_amt,
	date_dim.d_day_name,
	customer_address.ca_state,
	store_sales.ss_store_sk,
	web_sales.ws_ext_list_price,
	customer_address.ca_country,
	store_sales.ss_item_sk,
	store_sales.ss_customer_sk,
	date_dim.d_qoy,
	web_sales.ws_list_price,
	customer_address.ca_street_number,
	customer_address.ca_address_sk,
	date_dim.d_dom,
	web_sales.ws_ship_customer_sk,
	store_sales.ss_sold_date_sk,
	web_sales.ws_ship_addr_sk,
	web_sales.ws_order_number,
	web_sales.ws_web_page_sk,
	web_sales.ws_bill_addr_sk,
	store_sales.ss_cdemo_sk,
	store_sales.ss_coupon_amt,
	web_sales.ws_ext_ship_cost,
	customer_address.ca_street_name,
	store_sales.ss_addr_sk,
	customer_address.ca_zip,
	web_sales.ws_warehouse_sk,
	date_dim.d_week_seq,
	web_sales.ws_ship_date_sk,
	date_dim.d_year,
	web_sales.ws_bill_customer_sk,
	web_sales.ws_net_profit,
	store_sales.ss_ext_sales_price,
	customer_address.ca_county,
	store_sales.ss_list_price,
	store_sales.ss_ticket_number,
	web_sales.ws_sales_price,
	web_sales.ws_sold_time_sk,
	date_dim.d_month_seq,
	customer_address.ca_street_type,
	store_sales.ss_quantity,
	store_sales.ss_ext_tax,
	store_sales.ss_wholesale_cost,
	web_sales.ws_ext_wholesale_cost,
	web_sales.ws_item_sk,
	web_sales.ws_sold_date_sk,
	store_sales.ss_ext_wholesale_cost,
	web_sales.ws_web_site_sk,
	customer_address.ca_gmt_offset,
	web_sales.ws_net_paid,
	web_sales.ws_promo_sk,
	store_sales.ss_ext_list_price,
	customer_address.ca_suite_number,
	web_sales.ws_quantity,
	store_sales.ss_sales_price,
	date_dim.d_moy,
	store_sales.ss_sold_time_sk,
	date_dim.d_date,
	store_sales.ss_net_profit,
	web_sales.ws_ext_sales_price,
	store_sales.ss_hdemo_sk,
	web_sales.ws_ext_discount_amt,
	date_dim.d_date_sk,
	store_sales.ss_net_paid
FROM
	customer_address,
	date_dim,
	store_sales,
	web_sales
WHERE
	date_dim.d_qoy = 1
	AND date_dim.d_qoy = 3
	AND date_dim.d_qoy = 2
	AND web_sales.ws_sold_date_sk = date_dim.d_date_sk
	AND store_sales.ss_sold_date_sk = date_dim.d_date_sk
	AND date_dim.d_year = 2000
	AND store_sales.ss_addr_sk = customer_address.ca_address_sk
	AND web_sales.ws_bill_addr_sk = customer_address.ca_address_sk