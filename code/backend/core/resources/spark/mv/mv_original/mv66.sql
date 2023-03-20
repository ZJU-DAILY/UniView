SELECT
	customer_address.ca_location_type,
	date_dim.d_month_seq,
	catalog_sales.cs_ship_customer_sk,
	customer_address.ca_street_type,
	catalog_sales.cs_ext_ship_cost,
	date_dim.d_date_sk,
	customer.c_birth_month,
	date_dim.d_qoy,
	catalog_sales.cs_catalog_page_sk,
	catalog_sales.cs_call_center_sk,
	customer_address.ca_county,
	customer_address.ca_city,
	date_dim.d_date,
	customer_address.ca_street_name,
	catalog_sales.cs_order_number,
	catalog_sales.cs_bill_cdemo_sk,
	catalog_sales.cs_ext_sales_price,
	catalog_sales.cs_wholesale_cost,
	customer.c_current_addr_sk,
	customer.c_birth_country,
	customer.c_first_name,
	customer.c_last_review_date_sk,
	catalog_sales.cs_bill_addr_sk,
	catalog_sales.cs_net_paid,
	customer_address.ca_zip,
	catalog_sales.cs_ext_wholesale_cost,
	catalog_sales.cs_ship_date_sk,
	date_dim.d_moy,
	date_dim.d_week_seq,
	customer.c_current_cdemo_sk,
	date_dim.d_year,
	customer_address.ca_address_sk,
	catalog_sales.cs_promo_sk,
	catalog_sales.cs_item_sk,
	customer.c_last_name,
	catalog_sales.cs_bill_customer_sk,
	catalog_sales.cs_ship_mode_sk,
	customer.c_birth_day,
	customer.c_email_address,
	catalog_sales.cs_ext_list_price,
	customer.c_first_shipto_date_sk,
	date_dim.d_quarter_name,
	catalog_sales.cs_bill_hdemo_sk,
	customer_address.ca_street_number,
	customer.c_customer_sk,
	customer.c_birth_year,
	customer_address.ca_state,
	catalog_sales.cs_warehouse_sk,
	customer_address.ca_gmt_offset,
	catalog_sales.cs_sold_time_sk,
	customer.c_customer_id,
	customer.c_login,
	catalog_sales.cs_sales_price,
	customer.c_current_hdemo_sk,
	catalog_sales.cs_quantity,
	customer_address.ca_country,
	catalog_sales.cs_coupon_amt,
	catalog_sales.cs_list_price,
	customer_address.ca_suite_number,
	catalog_sales.cs_net_profit,
	catalog_sales.cs_ship_addr_sk,
	customer.c_first_sales_date_sk,
	date_dim.d_day_name,
	date_dim.d_dom,
	customer.c_salutation,
	catalog_sales.cs_net_paid_inc_tax,
	catalog_sales.cs_ext_discount_amt,
	date_dim.d_dow,
	customer.c_preferred_cust_flag,
	catalog_sales.cs_sold_date_sk
FROM
	date_dim,
	customer,
	customer_address,
	catalog_sales
WHERE
	date_dim.d_qoy = 2
	AND date_dim.d_year = 2001
	AND catalog_sales.cs_sold_date_sk = date_dim.d_date_sk
	AND catalog_sales.cs_bill_customer_sk = customer.c_customer_sk
	AND customer.c_current_addr_sk = customer_address.ca_address_sk