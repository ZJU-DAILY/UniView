SELECT
	customer.c_first_shipto_date_sk,
	customer.c_birth_year,
	customer.c_current_addr_sk,
	store_sales.ss_promo_sk,
	store_sales.ss_ext_discount_amt,
	store_sales.ss_store_sk,
	customer.c_birth_country,
	store_sales.ss_customer_sk,
	store_sales.ss_item_sk,
	customer.c_customer_id,
	customer.c_birth_month,
	store_sales.ss_sold_date_sk,
	customer.c_first_name,
	store_sales.ss_cdemo_sk,
	store_sales.ss_coupon_amt,
	customer.c_first_sales_date_sk,
	customer.c_last_name,
	store_sales.ss_addr_sk,
	store_sales.ss_ext_sales_price,
	store_sales.ss_list_price,
	store_sales.ss_ticket_number,
	store_sales.ss_quantity,
	store_sales.ss_ext_tax,
	store_sales.ss_wholesale_cost,
	customer.c_last_review_date_sk,
	store_sales.ss_ext_wholesale_cost,
	customer.c_current_cdemo_sk,
	store_sales.ss_ext_list_price,
	customer.c_email_address,
	customer.c_customer_sk,
	store_sales.ss_sales_price,
	store_sales.ss_sold_time_sk,
	customer.c_current_hdemo_sk,
	customer.c_birth_day,
	customer.c_login,
	store_sales.ss_net_profit,
	customer.c_salutation,
	customer.c_preferred_cust_flag,
	store_sales.ss_hdemo_sk,
	store_sales.ss_net_paid
FROM
	store_sales,
	customer
WHERE
	customer.c_customer_sk = store_sales.ss_customer_sk