CREATE MATERIALIZED VIEW IF NOT EXISTS mv199
PARTITIONED BY (cs_sold_date_sk)
AS
SELECT
	item.i_brand,
	catalog_sales.cs_ship_customer_sk,
	catalog_sales.cs_ext_ship_cost,
	catalog_sales.cs_catalog_page_sk,
	item.i_brand_id,
	catalog_sales.cs_call_center_sk,
	item.i_manager_id,
	item.i_class_id,
	item.i_item_desc,
	catalog_sales.cs_order_number,
	item.i_class,
	catalog_sales.cs_bill_cdemo_sk,
	catalog_sales.cs_ext_sales_price,
	catalog_sales.cs_wholesale_cost,
	item.i_category,
	catalog_sales.cs_bill_addr_sk,
	catalog_sales.cs_net_paid,
	item.i_size,
	catalog_sales.cs_ext_wholesale_cost,
	catalog_sales.cs_ship_date_sk,
	item.i_item_id,
	item.i_current_price,
	catalog_sales.cs_promo_sk,
	catalog_sales.cs_item_sk,
	catalog_sales.cs_ship_mode_sk,
	catalog_sales.cs_bill_customer_sk,
	catalog_sales.cs_ext_list_price,
	item.i_manufact,
	item.i_units,
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
	catalog_sales.cs_ship_addr_sk,
	item.i_category_id,
	item.i_manufact_id,
	catalog_sales.cs_net_paid_inc_tax,
	catalog_sales.cs_ext_discount_amt,
	catalog_sales.cs_sold_date_sk
FROM
	catalog_sales,
	item
WHERE
	item.i_category = 'Women'
	AND catalog_sales.cs_item_sk = item.i_item_sk
	AND item.i_class = 'maternity'
DISTRIBUTE BY cs_sold_date_sk;