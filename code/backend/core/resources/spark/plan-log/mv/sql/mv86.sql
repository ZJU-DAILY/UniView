SELECT
	item.i_manufact,
	customer_address.ca_location_type,
	customer_address.ca_city,
	store_sales.ss_promo_sk,
	date_dim.d_quarter_name,
	date_dim.d_day_name,
	store_sales.ss_store_sk,
	customer_address.ca_state,
	customer_address.ca_country,
	item.i_manager_id,
	store_sales.ss_customer_sk,
	date_dim.d_qoy,
	item.i_class_id,
	customer_address.ca_street_number,
	customer_address.ca_address_sk,
	store_sales.ss_sold_date_sk,
	customer.c_first_name,
	customer.c_first_sales_date_sk,
	store_sales.ss_cdemo_sk,
	customer_address.ca_street_name,
	customer.c_last_name,
	customer_address.ca_zip,
	date_dim.d_week_seq,
	date_dim.d_year,
	item.i_item_sk,
	item.i_product_name,
	store_sales.ss_ext_sales_price,
	customer_address.ca_county,
	item.i_wholesale_cost,
	store_sales.ss_ext_tax,
	store_sales.ss_ext_wholesale_cost,
	customer.c_current_cdemo_sk,
	customer_address.ca_gmt_offset,
	store_sales.ss_ext_list_price,
	customer.c_customer_sk,
	item.i_class,
	store_sales.ss_sold_time_sk,
	customer.c_current_hdemo_sk,
	customer.c_birth_day,
	item.i_category,
	customer.c_login,
	customer.c_salutation,
	store_sales.ss_hdemo_sk,
	date_dim.d_date_sk,
	item.i_category_id,
	customer.c_first_shipto_date_sk,
	date_dim.d_dow,
	customer.c_birth_year,
	customer.c_current_addr_sk,
	store_sales.ss_ext_discount_amt,
	customer.c_birth_country,
	store_sales.ss_item_sk,
	customer.c_birth_month,
	customer.c_customer_id,
	date_dim.d_dom,
	item.i_color,
	item.i_size,
	store_sales.ss_coupon_amt,
	store_sales.ss_addr_sk,
	item.i_units,
	item.i_brand_id,
	item.i_brand,
	store_sales.ss_list_price,
	store_sales.ss_ticket_number,
	item.i_current_price,
	customer_address.ca_street_type,
	date_dim.d_month_seq,
	item.i_item_id,
	store_sales.ss_quantity,
	customer.c_last_review_date_sk,
	item.i_manufact_id,
	customer.c_email_address,
	customer_address.ca_suite_number,
	store_sales.ss_sales_price,
	date_dim.d_moy,
	item.i_item_desc,
	date_dim.d_date,
	store_sales.ss_net_profit,
	customer.c_preferred_cust_flag,
	store_sales.ss_wholesale_cost,
	store_sales.ss_net_paid
FROM
	item,
	customer_address,
	customer,
	date_dim,
	store_sales
WHERE
	date_dim.d_date_sk = store_sales.ss_sold_date_sk
	AND date_dim.d_year = 1998
	AND date_dim.d_moy = 11
	AND store_sales.ss_item_sk = item.i_item_sk
	AND store_sales.ss_customer_sk = customer.c_customer_sk
	AND item.i_manager_id = 8
	AND customer.c_current_addr_sk = customer_address.ca_address_sk