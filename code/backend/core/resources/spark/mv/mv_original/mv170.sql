SELECT
	customer_address.ca_location_type,
	web_sales.ws_ship_date_sk,
	web_sales.ws_ship_mode_sk,
	customer_address.ca_street_type,
	web_sales.ws_order_number,
	customer.c_birth_month,
	web_sales.ws_promo_sk,
	customer_address.ca_county,
	web_sales.ws_ship_hdemo_sk,
	customer_address.ca_city,
	web_sales.ws_web_site_sk,
	web_sales.ws_sold_time_sk,
	customer_address.ca_street_name,
	customer.c_current_addr_sk,
	web_sales.ws_sales_price,
	customer.c_birth_country,
	web_sales.ws_ext_list_price,
	customer.c_first_name,
	customer.c_last_review_date_sk,
	customer.c_salutation,
	customer_address.ca_zip,
	web_sales.ws_warehouse_sk,
	web_sales.ws_quantity,
	web_sales.ws_bill_addr_sk,
	web_sales.ws_web_page_sk,
	customer.c_current_cdemo_sk,
	web_sales.ws_ext_ship_cost,
	web_sales.ws_net_paid,
	customer_address.ca_address_sk,
	customer.c_last_name,
	customer.c_birth_day,
	customer.c_email_address,
	web_sales.ws_ship_customer_sk,
	web_sales.ws_ext_discount_amt,
	customer.c_first_shipto_date_sk,
	web_sales.ws_ext_wholesale_cost,
	web_sales.ws_wholesale_cost,
	customer_address.ca_street_number,
	customer.c_customer_sk,
	customer_address.ca_state,
	web_sales.ws_list_price,
	customer.c_birth_year,
	web_sales.ws_sold_date_sk,
	customer_address.ca_gmt_offset,
	customer.c_customer_id,
	customer.c_login,
	web_sales.ws_ext_sales_price,
	web_sales.ws_ship_addr_sk,
	customer_address.ca_country,
	customer_address.ca_suite_number,
	customer.c_first_sales_date_sk,
	web_sales.ws_bill_customer_sk,
	web_sales.ws_net_profit,
	web_sales.ws_item_sk,
	customer.c_preferred_cust_flag,
	customer.c_current_hdemo_sk
FROM
	web_sales,
	customer_address,
	customer
WHERE
	customer.c_current_addr_sk = customer_address.ca_address_sk
	AND web_sales.ws_bill_customer_sk = customer.c_customer_sk