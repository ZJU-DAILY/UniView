SELECT
	date_dim.d_month_seq,
	catalog_sales.cs_ship_customer_sk,
	catalog_sales.cs_ext_ship_cost,
	date_dim.d_date_sk,
	catalog_sales.cs_catalog_page_sk,
	date_dim.d_qoy,
	customer.c_birth_month,
	catalog_sales.cs_call_center_sk,
	date_dim.d_date,
	catalog_sales.cs_order_number,
	catalog_sales.cs_bill_cdemo_sk,
	catalog_sales.cs_ext_sales_price,
	catalog_sales.cs_wholesale_cost,
	customer.c_birth_country,
	customer.c_current_addr_sk,
	customer.c_first_name,
	catalog_sales.cs_bill_addr_sk,
	catalog_sales.cs_net_paid,
	customer.c_last_review_date_sk,
	catalog_sales.cs_ext_wholesale_cost,
	catalog_sales.cs_ship_date_sk,
	date_dim.d_moy,
	date_dim.d_week_seq,
	customer.c_current_cdemo_sk,
	date_dim.d_year,
	catalog_sales.cs_promo_sk,
	catalog_sales.cs_item_sk,
	catalog_sales.cs_ship_mode_sk,
	catalog_sales.cs_bill_customer_sk,
	customer.c_last_name,
	catalog_sales.cs_ext_list_price,
	customer.c_birth_day,
	customer.c_email_address,
	customer.c_first_shipto_date_sk,
	date_dim.d_quarter_name,
	catalog_sales.cs_bill_hdemo_sk,
	customer.c_customer_sk,
	catalog_sales.cs_warehouse_sk,
	customer.c_birth_year,
	catalog_sales.cs_sold_time_sk,
	customer.c_customer_id,
	customer.c_login,
	catalog_sales.cs_sales_price,
	catalog_sales.cs_quantity,
	catalog_sales.cs_coupon_amt,
	customer.c_current_hdemo_sk,
	catalog_sales.cs_list_price,
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
	catalog_sales,
	customer
WHERE
	catalog_sales.cs_sold_date_sk = date_dim.d_date_sk
	AND catalog_sales.cs_bill_customer_sk = customer.c_customer_sk
	AND (((date_dim.d_month_seq >= 1200) AND (date_dim.d_month_seq <= 1211)))