CREATE MATERIALIZED VIEW IF NOT EXISTS mv100
AS
SELECT
	catalog_sales.cs_ship_customer_sk,
	store_sales.ss_hdemo_sk,
	item.i_brand_id,
	catalog_sales.cs_catalog_page_sk,
	customer.c_birth_month,
	date_dim.d_qoy,
	catalog_sales.cs_call_center_sk,
	store_sales.ss_list_price,
	item.i_manager_id,
	store_sales.ss_store_sk,
	catalog_sales.cs_bill_cdemo_sk,
	item.i_class,
	store_sales.ss_ext_wholesale_cost,
	customer.c_birth_country,
	catalog_sales.cs_sold_date_sk,
	customer.c_last_review_date_sk,
	catalog_sales.cs_bill_addr_sk,
	item.i_item_id,
	date_dim.d_moy,
	item.i_current_price,
	store_sales.ss_customer_sk,
	date_dim.d_week_seq,
	customer.c_current_cdemo_sk,
	catalog_sales.cs_item_sk,
	catalog_sales.cs_bill_customer_sk,
	customer.c_last_name,
	item.i_manufact,
	customer.c_birth_day,
	customer.c_email_address,
	item.i_units,
	date_dim.d_quarter_name,
	store_sales.ss_sales_price,
	item.i_product_name,
	customer.c_birth_year,
	catalog_sales.cs_sold_time_sk,
	store_sales.ss_ext_discount_amt,
	store_sales.ss_promo_sk,
	catalog_sales.cs_sales_price,
	catalog_sales.cs_quantity,
	store_sales.ss_wholesale_cost,
	catalog_sales.cs_coupon_amt,
	item.i_color,
	date_dim.d_dom,
	item.i_manufact_id,
	catalog_sales.cs_net_paid_inc_tax,
	catalog_sales.cs_ext_discount_amt,
	store_sales.ss_net_paid,
	store_sales.ss_quantity,
	customer.c_preferred_cust_flag,
	item.i_brand,
	date_dim.d_month_seq,
	store_sales.ss_ext_sales_price,
	catalog_sales.cs_ext_ship_cost,
	date_dim.d_date_sk,
	store_sales.ss_ticket_number,
	store_sales.ss_cdemo_sk,
	date_dim.d_date,
	item.i_class_id,
	item.i_item_desc,
	catalog_sales.cs_order_number,
	catalog_sales.cs_wholesale_cost,
	catalog_sales.cs_ext_sales_price,
	customer.c_current_addr_sk,
	item.i_category,
	customer.c_first_name,
	catalog_sales.cs_net_paid,
	item.i_size,
	catalog_sales.cs_ext_wholesale_cost,
	catalog_sales.cs_ship_date_sk,
	store_sales.ss_ext_list_price,
	date_dim.d_year,
	catalog_sales.cs_promo_sk,
	catalog_sales.cs_ship_mode_sk,
	catalog_sales.cs_ext_list_price,
	store_sales.ss_net_profit,
	store_sales.ss_sold_date_sk,
	customer.c_first_shipto_date_sk,
	store_sales.ss_addr_sk,
	catalog_sales.cs_bill_hdemo_sk,
	store_sales.ss_item_sk,
	store_sales.ss_coupon_amt,
	customer.c_customer_sk,
	item.i_item_sk,
	store_sales.ss_sold_time_sk,
	catalog_sales.cs_warehouse_sk,
	item.i_wholesale_cost,
	customer.c_customer_id,
	customer.c_login,
	catalog_sales.cs_list_price,
	catalog_sales.cs_net_profit,
	catalog_sales.cs_ship_addr_sk,
	customer.c_first_sales_date_sk,
	date_dim.d_day_name,
	item.i_category_id,
	store_sales.ss_ext_tax,
	customer.c_salutation,
	date_dim.d_dow,
	customer.c_current_hdemo_sk
FROM
	date_dim,
	store_sales,
	customer,
	catalog_sales,
	item
WHERE
	catalog_sales.cs_item_sk = item.i_item_sk
	AND catalog_sales.cs_bill_customer_sk = customer.c_customer_sk
	AND date_dim.d_moy = 2
	AND (((date_dim.d_year = 2000) OR (date_dim.d_year = 2001) OR (date_dim.d_year = 2002) OR (date_dim.d_year = 2003)))
	AND date_dim.d_year = 2000
	AND store_sales.ss_sold_date_sk = date_dim.d_date_sk
	AND catalog_sales.cs_sold_date_sk = date_dim.d_date_sk
	AND store_sales.ss_item_sk = item.i_item_sk
	AND store_sales.ss_customer_sk = customer.c_customer_sk;