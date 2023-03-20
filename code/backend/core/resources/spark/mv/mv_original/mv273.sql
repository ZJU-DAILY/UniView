SELECT
	customer_demographics.cd_purchase_estimate,
	catalog_sales.cs_ship_customer_sk,
	web_sales.ws_ship_mode_sk,
	store_sales.ss_hdemo_sk,
	catalog_sales.cs_catalog_page_sk,
	customer.c_birth_month,
	web_sales.ws_order_number,
	date_dim.d_qoy,
	catalog_sales.cs_call_center_sk,
	customer_demographics.cd_dep_count,
	web_sales.ws_ship_hdemo_sk,
	customer_address.ca_city,
	web_sales.ws_bill_customer_sk,
	web_sales.ws_web_site_sk,
	store_sales.ss_list_price,
	web_sales.ws_sold_time_sk,
	store_sales.ss_store_sk,
	customer_address.ca_street_name,
	catalog_sales.cs_bill_cdemo_sk,
	store_sales.ss_ext_wholesale_cost,
	customer.c_birth_country,
	web_sales.ws_sales_price,
	catalog_sales.cs_sold_date_sk,
	customer.c_last_review_date_sk,
	catalog_sales.cs_bill_addr_sk,
	customer_demographics.cd_demo_sk,
	web_sales.ws_bill_addr_sk,
	date_dim.d_moy,
	store_sales.ss_customer_sk,
	date_dim.d_week_seq,
	customer.c_current_cdemo_sk,
	web_sales.ws_ext_ship_cost,
	catalog_sales.cs_item_sk,
	catalog_sales.cs_bill_customer_sk,
	customer.c_last_name,
	customer.c_birth_day,
	customer.c_email_address,
	web_sales.ws_ship_customer_sk,
	customer_demographics.cd_gender,
	date_dim.d_quarter_name,
	web_sales.ws_wholesale_cost,
	store_sales.ss_sales_price,
	customer_address.ca_street_number,
	customer_address.ca_state,
	customer.c_birth_year,
	customer_demographics.cd_dep_employed_count,
	catalog_sales.cs_sold_time_sk,
	store_sales.ss_ext_discount_amt,
	store_sales.ss_promo_sk,
	catalog_sales.cs_sales_price,
	store_sales.ss_wholesale_cost,
	catalog_sales.cs_quantity,
	customer_address.ca_country,
	catalog_sales.cs_coupon_amt,
	web_sales.ws_ext_sales_price,
	customer_address.ca_suite_number,
	date_dim.d_dom,
	catalog_sales.cs_net_paid_inc_tax,
	catalog_sales.cs_ext_discount_amt,
	store_sales.ss_net_paid,
	store_sales.ss_quantity,
	customer.c_preferred_cust_flag,
	customer_address.ca_location_type,
	date_dim.d_month_seq,
	web_sales.ws_ship_date_sk,
	customer_address.ca_street_type,
	store_sales.ss_ext_sales_price,
	customer_demographics.cd_dep_college_count,
	catalog_sales.cs_ext_ship_cost,
	date_dim.d_date_sk,
	web_sales.ws_promo_sk,
	customer_address.ca_county,
	store_sales.ss_ticket_number,
	store_sales.ss_cdemo_sk,
	date_dim.d_date,
	catalog_sales.cs_order_number,
	catalog_sales.cs_wholesale_cost,
	catalog_sales.cs_ext_sales_price,
	customer_demographics.cd_education_status,
	customer.c_current_addr_sk,
	web_sales.ws_ext_list_price,
	customer.c_first_name,
	catalog_sales.cs_net_paid,
	customer_address.ca_zip,
	catalog_sales.cs_ext_wholesale_cost,
	web_sales.ws_warehouse_sk,
	catalog_sales.cs_ship_date_sk,
	web_sales.ws_quantity,
	web_sales.ws_web_page_sk,
	store_sales.ss_ext_list_price,
	date_dim.d_year,
	customer_address.ca_address_sk,
	web_sales.ws_net_paid,
	catalog_sales.cs_ship_mode_sk,
	catalog_sales.cs_ext_list_price,
	catalog_sales.cs_promo_sk,
	store_sales.ss_net_profit,
	store_sales.ss_sold_date_sk,
	web_sales.ws_ext_discount_amt,
	customer.c_first_shipto_date_sk,
	store_sales.ss_addr_sk,
	web_sales.ws_ext_wholesale_cost,
	catalog_sales.cs_bill_hdemo_sk,
	store_sales.ss_item_sk,
	store_sales.ss_coupon_amt,
	customer.c_customer_sk,
	customer_demographics.cd_marital_status,
	store_sales.ss_sold_time_sk,
	catalog_sales.cs_warehouse_sk,
	web_sales.ws_list_price,
	customer_demographics.cd_credit_rating,
	web_sales.ws_sold_date_sk,
	customer_address.ca_gmt_offset,
	customer.c_customer_id,
	customer.c_login,
	web_sales.ws_ship_addr_sk,
	catalog_sales.cs_list_price,
	catalog_sales.cs_net_profit,
	catalog_sales.cs_ship_addr_sk,
	customer.c_first_sales_date_sk,
	date_dim.d_day_name,
	store_sales.ss_ext_tax,
	customer.c_salutation,
	web_sales.ws_net_profit,
	web_sales.ws_item_sk,
	date_dim.d_dow,
	customer.c_current_hdemo_sk
FROM
	date_dim,
	store_sales,
	web_sales,
	customer,
	customer_address,
	catalog_sales,
	customer_demographics
WHERE
	customer.c_customer_sk = web_sales.ws_bill_customer_sk
	AND customer.c_customer_sk = catalog_sales.cs_ship_customer_sk
	AND customer.c_customer_sk = store_sales.ss_customer_sk
	AND (((customer_address.ca_state = 'KY') OR (customer_address.ca_state = 'GA') OR (customer_address.ca_state = 'NM')))
	AND customer.c_current_addr_sk = customer_address.ca_address_sk
	AND date_dim.d_moy <= 6
	AND date_dim.d_moy >= 4
	AND store_sales.ss_sold_date_sk = date_dim.d_date_sk
	AND catalog_sales.cs_sold_date_sk = date_dim.d_date_sk
	AND web_sales.ws_sold_date_sk = date_dim.d_date_sk
	AND date_dim.d_year = 2001
	AND customer.c_current_cdemo_sk = customer_demographics.cd_demo_sk