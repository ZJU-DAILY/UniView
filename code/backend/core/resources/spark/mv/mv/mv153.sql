CREATE MATERIALIZED VIEW IF NOT EXISTS mv153
PARTITIONED BY (cs_sold_date_sk)
AS
SELECT
	item.i_brand,
	date_dim.d_month_seq,
	catalog_sales.cs_ship_customer_sk,
	inventory.inv_warehouse_sk,
	catalog_sales.cs_ext_ship_cost,
	date_dim.d_date_sk,
	catalog_sales.cs_catalog_page_sk,
	date_dim.d_qoy,
	item.i_brand_id,
	inventory.inv_quantity_on_hand,
	catalog_sales.cs_call_center_sk,
	date_dim.d_date,
	item.i_manager_id,
	item.i_class_id,
	item.i_item_desc,
	catalog_sales.cs_order_number,
	catalog_sales.cs_bill_cdemo_sk,
	catalog_sales.cs_ext_sales_price,
	catalog_sales.cs_wholesale_cost,
	item.i_class,
	item.i_category,
	catalog_sales.cs_bill_addr_sk,
	catalog_sales.cs_net_paid,
	item.i_size,
	catalog_sales.cs_ext_wholesale_cost,
	catalog_sales.cs_ship_date_sk,
	item.i_item_id,
	date_dim.d_moy,
	item.i_current_price,
	date_dim.d_week_seq,
	date_dim.d_year,
	catalog_sales.cs_promo_sk,
	catalog_sales.cs_item_sk,
	catalog_sales.cs_ship_mode_sk,
	catalog_sales.cs_bill_customer_sk,
	catalog_sales.cs_ext_list_price,
	item.i_manufact,
	item.i_units,
	date_dim.d_quarter_name,
	catalog_sales.cs_bill_hdemo_sk,
	inventory.inv_date_sk,
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
	catalog_sales.cs_ship_addr_sk,
	date_dim.d_day_name,
	item.i_category_id,
	date_dim.d_dom,
	inventory.inv_item_sk,
	item.i_manufact_id,
	catalog_sales.cs_net_paid_inc_tax,
	catalog_sales.cs_ext_discount_amt,
	date_dim.d_dow,
	catalog_sales.cs_sold_date_sk
FROM
	date_dim,
	inventory,
	catalog_sales,
	item
WHERE
	inventory.inv_date_sk = date_dim.d_date_sk
	AND item.i_item_sk = inventory.inv_item_sk
	AND (((inventory.inv_quantity_on_hand >= 100) AND (inventory.inv_quantity_on_hand <= 500)))
	AND (((date_dim.d_date >= cast('1970-01-01' as date) + interval 10988 days) AND (date_dim.d_date <= cast('1970-01-01' as date) + interval 11048 days)))
	AND item.i_item_sk = catalog_sales.cs_item_sk
	AND (((item.i_manufact_id = 677) OR (item.i_manufact_id = 940) OR (item.i_manufact_id = 694) OR (item.i_manufact_id = 808)))
DISTRIBUTE BY cs_sold_date_sk;