SELECT
	item.i_brand,
	date_dim.d_month_seq,
	store_sales.ss_hdemo_sk,
	store_sales.ss_ext_sales_price,
	item.i_brand_id,
	date_dim.d_date_sk,
	customer.c_birth_month,
	date_dim.d_qoy,
	store_sales.ss_ticket_number,
	store_sales.ss_cdemo_sk,
	date_dim.d_date,
	store_sales.ss_list_price,
	item.i_manager_id,
	item.i_class_id,
	store_sales.ss_store_sk,
	item.i_item_desc,
	item.i_class,
	store_sales.ss_ext_wholesale_cost,
	customer.c_current_addr_sk,
	customer.c_birth_country,
	item.i_category,
	customer.c_first_name,
	customer.c_last_review_date_sk,
	item.i_size,
	customer.c_salutation,
	item.i_item_id,
	date_dim.d_moy,
	item.i_current_price,
	store_sales.ss_ext_list_price,
	date_dim.d_week_seq,
	store_sales.ss_customer_sk,
	customer.c_current_cdemo_sk,
	date_dim.d_year,
	customer.c_last_name,
	item.i_manufact,
	store_sales.ss_net_profit,
	item.i_units,
	customer.c_birth_day,
	store_sales.ss_sold_date_sk,
	customer.c_email_address,
	customer.c_first_shipto_date_sk,
	store_sales.ss_addr_sk,
	date_dim.d_quarter_name,
	store_sales.ss_sales_price,
	store_sales.ss_item_sk,
	store_sales.ss_coupon_amt,
	customer.c_customer_sk,
	item.i_item_sk,
	store_sales.ss_sold_time_sk,
	item.i_product_name,
	item.i_wholesale_cost,
	customer.c_birth_year,
	store_sales.ss_ext_discount_amt,
	customer.c_customer_id,
	store_sales.ss_promo_sk,
	item.i_color,
	store_sales.ss_wholesale_cost,
	customer.c_login,
	customer.c_first_sales_date_sk,
	date_dim.d_day_name,
	item.i_category_id,
	date_dim.d_dom,
	store_sales.ss_ext_tax,
	item.i_manufact_id,
	store_sales.ss_net_paid,
	store_sales.ss_quantity,
	date_dim.d_dow,
	customer.c_preferred_cust_flag,
	customer.c_current_hdemo_sk
FROM
	date_dim,
	store_sales,
	item,
	customer
WHERE
	store_sales.ss_item_sk = item.i_item_sk
	AND store_sales.ss_customer_sk = customer.c_customer_sk
	AND date_dim.d_date_sk = store_sales.ss_sold_date_sk
	AND item.i_manager_id = 8
	AND date_dim.d_year = 1998
	AND date_dim.d_moy = 11