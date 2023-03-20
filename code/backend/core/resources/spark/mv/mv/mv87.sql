CREATE MATERIALIZED VIEW IF NOT EXISTS mv87
PARTITIONED BY (ss_sold_date_sk)
AS
SELECT
	store_sales.ss_hdemo_sk,
	item.i_brand_id,
	customer.c_birth_month,
	date_dim.d_qoy,
	store.s_store_name,
	customer_address.ca_city,
	store_sales.ss_list_price,
	item.i_manager_id,
	store_sales.ss_store_sk,
	customer_address.ca_street_name,
	item.i_class,
	store_sales.ss_ext_wholesale_cost,
	customer.c_birth_country,
	customer.c_last_review_date_sk,
	store.s_street_name,
	store.s_market_id,
	item.i_item_id,
	date_dim.d_moy,
	item.i_current_price,
	store_sales.ss_customer_sk,
	date_dim.d_week_seq,
	customer.c_current_cdemo_sk,
	customer.c_last_name,
	item.i_manufact,
	store.s_state,
	customer.c_birth_day,
	customer.c_email_address,
	item.i_units,
	date_dim.d_quarter_name,
	store_sales.ss_sales_price,
	customer_address.ca_street_number,
	item.i_product_name,
	customer.c_birth_year,
	customer_address.ca_state,
	store.s_store_sk,
	store_sales.ss_ext_discount_amt,
	store_sales.ss_promo_sk,
	item.i_color,
	store_sales.ss_wholesale_cost,
	customer_address.ca_country,
	store.s_number_employees,
	customer_address.ca_suite_number,
	date_dim.d_dom,
	store.s_county,
	item.i_manufact_id,
	store.s_street_type,
	store_sales.ss_net_paid,
	store_sales.ss_quantity,
	customer.c_preferred_cust_flag,
	item.i_brand,
	customer_address.ca_location_type,
	date_dim.d_month_seq,
	store.s_suite_number,
	customer_address.ca_street_type,
	store_sales.ss_ext_sales_price,
	date_dim.d_date_sk,
	customer_address.ca_county,
	store_sales.ss_ticket_number,
	store.s_zip,
	store_sales.ss_cdemo_sk,
	date_dim.d_date,
	store.s_store_id,
	item.i_class_id,
	item.i_item_desc,
	customer.c_current_addr_sk,
	item.i_category,
	customer.c_first_name,
	store.s_company_name,
	store.s_city,
	item.i_size,
	customer_address.ca_zip,
	store_sales.ss_ext_list_price,
	store.s_gmt_offset,
	date_dim.d_year,
	customer_address.ca_address_sk,
	store_sales.ss_net_profit,
	store_sales.ss_sold_date_sk,
	customer.c_first_shipto_date_sk,
	store_sales.ss_addr_sk,
	store.s_company_id,
	store_sales.ss_item_sk,
	store_sales.ss_coupon_amt,
	customer.c_customer_sk,
	item.i_item_sk,
	store_sales.ss_sold_time_sk,
	item.i_wholesale_cost,
	customer_address.ca_gmt_offset,
	customer.c_customer_id,
	customer.c_login,
	customer.c_first_sales_date_sk,
	store.s_street_number,
	date_dim.d_day_name,
	item.i_category_id,
	store_sales.ss_ext_tax,
	customer.c_salutation,
	date_dim.d_dow,
	customer.c_current_hdemo_sk
FROM
	date_dim,
	store_sales,
	store,
	customer,
	customer_address,
	item
WHERE
	store_sales.ss_store_sk = store.s_store_sk
	AND store_sales.ss_item_sk = item.i_item_sk
	AND store_sales.ss_customer_sk = customer.c_customer_sk
	AND date_dim.d_date_sk = store_sales.ss_sold_date_sk
	AND item.i_manager_id = 8
	AND customer.c_current_addr_sk = customer_address.ca_address_sk
	AND date_dim.d_year = 1998
	AND date_dim.d_moy = 11
DISTRIBUTE BY ss_sold_date_sk;