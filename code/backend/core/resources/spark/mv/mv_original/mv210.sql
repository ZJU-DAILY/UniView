SELECT
	item.i_brand,
	date_dim.d_month_seq,
	catalog_sales.cs_ship_customer_sk,
	catalog_sales.cs_ext_ship_cost,
	date_dim.d_date_sk,
	catalog_sales.cs_catalog_page_sk,
	date_dim.d_qoy,
	item.i_brand_id,
	catalog_sales.cs_call_center_sk,
	date_dim.d_date,
	item.i_manager_id,
	item.i_class_id,
	call_center.cc_manager,
	item.i_item_desc,
	catalog_sales.cs_order_number,
	catalog_sales.cs_bill_cdemo_sk,
	catalog_sales.cs_ext_sales_price,
	catalog_sales.cs_wholesale_cost,
	item.i_class,
	item.i_category,
	call_center.cc_call_center_id,
	catalog_sales.cs_bill_addr_sk,
	catalog_sales.cs_net_paid,
	item.i_size,
	catalog_sales.cs_ext_wholesale_cost,
	catalog_sales.cs_ship_date_sk,
	item.i_item_id,
	date_dim.d_moy,
	item.i_current_price,
	date_dim.d_week_seq,
	call_center.cc_call_center_sk,
	date_dim.d_year,
	catalog_sales.cs_promo_sk,
	catalog_sales.cs_item_sk,
	catalog_sales.cs_ship_mode_sk,
	catalog_sales.cs_bill_customer_sk,
	catalog_sales.cs_ext_list_price,
	item.i_manufact,
	item.i_units,
	call_center.cc_name,
	date_dim.d_quarter_name,
	catalog_sales.cs_bill_hdemo_sk,
	item.i_item_sk,
	item.i_product_name,
	catalog_sales.cs_warehouse_sk,
	item.i_wholesale_cost,
	catalog_sales.cs_sold_time_sk,
	item.i_color,
	catalog_sales.cs_sales_price,
	catalog_sales.cs_quantity,
	catalog_sales.cs_coupon_amt,
	catalog_sales.cs_list_price,
	catalog_sales.cs_net_profit,
	call_center.cc_county,
	catalog_sales.cs_ship_addr_sk,
	date_dim.d_day_name,
	item.i_category_id,
	date_dim.d_dom,
	item.i_manufact_id,
	catalog_sales.cs_net_paid_inc_tax,
	catalog_sales.cs_ext_discount_amt,
	date_dim.d_dow,
	catalog_sales.cs_sold_date_sk
FROM
	date_dim,
	call_center,
	catalog_sales,
	item
WHERE
	(((date_dim.d_year = 1999) OR (date_dim.d_year = 1998) OR (date_dim.d_year = 2000)))
	AND (((date_dim.d_moy = 12) OR (date_dim.d_moy = 1)))
	AND catalog_sales.cs_sold_date_sk = date_dim.d_date_sk
	AND catalog_sales.cs_call_center_sk = call_center.cc_call_center_sk
	AND item.i_item_sk = catalog_sales.cs_item_sk