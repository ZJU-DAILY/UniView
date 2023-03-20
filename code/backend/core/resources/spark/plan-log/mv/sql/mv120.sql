SELECT
	date_dim.d_dow,
	item.i_manufact,
	catalog_sales.cs_ext_ship_cost,
	catalog_sales.cs_wholesale_cost,
	catalog_sales.cs_quantity,
	date_dim.d_quarter_name,
	date_dim.d_day_name,
	date_dim.d_moy,
	customer_demographics.cd_education_status,
	catalog_sales.cs_net_profit,
	item.i_manager_id,
	catalog_sales.cs_net_paid,
	catalog_sales.cs_sold_time_sk,
	catalog_sales.cs_item_sk,
	catalog_sales.cs_list_price,
	item.i_class_id,
	customer_demographics.cd_dep_employed_count,
	date_dim.d_qoy,
	catalog_sales.cs_order_number,
	date_dim.d_dom,
	item.i_color,
	customer_demographics.cd_dep_college_count,
	customer_demographics.cd_demo_sk,
	catalog_sales.cs_ext_wholesale_cost,
	item.i_size,
	catalog_sales.cs_ext_sales_price,
	catalog_sales.cs_catalog_page_sk,
	item.i_units,
	catalog_sales.cs_call_center_sk,
	customer_demographics.cd_credit_rating,
	catalog_sales.cs_sales_price,
	catalog_sales.cs_ship_addr_sk,
	date_dim.d_week_seq,
	date_dim.d_year,
	item.i_item_sk,
	item.i_brand_id,
	catalog_sales.cs_net_paid_inc_tax,
	item.i_product_name,
	item.i_brand,
	item.i_current_price,
	date_dim.d_month_seq,
	item.i_wholesale_cost,
	item.i_item_id,
	catalog_sales.cs_bill_cdemo_sk,
	catalog_sales.cs_warehouse_sk,
	catalog_sales.cs_ship_customer_sk,
	customer_demographics.cd_purchase_estimate,
	item.i_manufact_id,
	customer_demographics.cd_marital_status,
	catalog_sales.cs_ship_mode_sk,
	customer_demographics.cd_dep_count,
	catalog_sales.cs_sold_date_sk,
	item.i_item_desc,
	catalog_sales.cs_coupon_amt,
	item.i_class,
	catalog_sales.cs_ext_list_price,
	catalog_sales.cs_ship_date_sk,
	catalog_sales.cs_bill_customer_sk,
	item.i_category,
	catalog_sales.cs_promo_sk,
	catalog_sales.cs_ext_discount_amt,
	customer_demographics.cd_gender,
	catalog_sales.cs_bill_addr_sk,
	date_dim.d_date,
	catalog_sales.cs_bill_hdemo_sk,
	date_dim.d_date_sk,
	item.i_category_id
FROM
	item,
	catalog_sales,
	customer_demographics,
	date_dim
WHERE
	customer_demographics.cd_gender = 'M'
	AND customer_demographics.cd_education_status = 'College'
	AND customer_demographics.cd_marital_status = 'S'
	AND catalog_sales.cs_bill_cdemo_sk = customer_demographics.cd_demo_sk
	AND date_dim.d_year = 2000
	AND catalog_sales.cs_sold_date_sk = date_dim.d_date_sk
	AND catalog_sales.cs_item_sk = item.i_item_sk