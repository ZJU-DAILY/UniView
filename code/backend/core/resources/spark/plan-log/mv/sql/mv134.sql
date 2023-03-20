SELECT
	date_dim.d_dow,
	item.i_manufact,
	catalog_sales.cs_ext_ship_cost,
	catalog_sales.cs_wholesale_cost,
	catalog_sales.cs_quantity,
	date_dim.d_quarter_name,
	date_dim.d_day_name,
	catalog_sales.cs_bill_addr_sk,
	catalog_sales.cs_net_profit,
	item.i_manager_id,
	catalog_sales.cs_net_paid,
	catalog_sales.cs_sold_time_sk,
	catalog_sales.cs_item_sk,
	date_dim.d_qoy,
	item.i_class_id,
	catalog_sales.cs_list_price,
	catalog_sales.cs_order_number,
	date_dim.d_dom,
	item.i_color,
	catalog_sales.cs_ext_wholesale_cost,
	item.i_size,
	catalog_sales.cs_ext_sales_price,
	catalog_sales.cs_catalog_page_sk,
	item.i_units,
	catalog_sales.cs_call_center_sk,
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
	item.i_manufact_id,
	catalog_sales.cs_ship_mode_sk,
	catalog_sales.cs_sold_date_sk,
	item.i_item_desc,
	item.i_class,
	date_dim.d_moy,
	catalog_sales.cs_ext_list_price,
	catalog_sales.cs_coupon_amt,
	date_dim.d_date,
	catalog_sales.cs_ext_discount_amt,
	item.i_category,
	catalog_sales.cs_ship_date_sk,
	catalog_sales.cs_bill_customer_sk,
	catalog_sales.cs_promo_sk,
	catalog_sales.cs_bill_hdemo_sk,
	date_dim.d_date_sk,
	item.i_category_id
FROM
	item,
	date_dim,
	catalog_sales
WHERE
	item.i_manufact_id = 977
	AND item.i_item_sk = catalog_sales.cs_item_sk
	AND catalog_sales.cs_item_sk = item.i_item_sk
	AND (((date_dim.d_date >= cast('1970-01-01' as date) + interval 10983 days) AND (date_dim.d_date <= cast('1970-01-01' as date) + interval 11073 days)))
	AND catalog_sales.cs_sold_date_sk = date_dim.d_date_sk