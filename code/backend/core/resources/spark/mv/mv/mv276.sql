CREATE MATERIALIZED VIEW IF NOT EXISTS mv276
PARTITIONED BY (cs_sold_date_sk)
AS
SELECT
	catalog_sales.cs_ship_customer_sk,
	inventory.inv_warehouse_sk,
	catalog_sales.cs_ext_ship_cost,
	catalog_sales.cs_catalog_page_sk,
	inventory.inv_quantity_on_hand,
	catalog_sales.cs_call_center_sk,
	catalog_sales.cs_bill_cdemo_sk,
	catalog_sales.cs_order_number,
	catalog_sales.cs_ext_sales_price,
	catalog_sales.cs_wholesale_cost,
	catalog_sales.cs_bill_addr_sk,
	catalog_sales.cs_net_paid,
	catalog_sales.cs_ext_wholesale_cost,
	catalog_sales.cs_ship_date_sk,
	catalog_sales.cs_promo_sk,
	catalog_sales.cs_item_sk,
	catalog_sales.cs_ship_mode_sk,
	catalog_sales.cs_bill_customer_sk,
	catalog_sales.cs_ext_list_price,
	catalog_sales.cs_bill_hdemo_sk,
	inventory.inv_date_sk,
	catalog_sales.cs_warehouse_sk,
	catalog_sales.cs_sold_time_sk,
	catalog_sales.cs_sales_price,
	catalog_sales.cs_quantity,
	catalog_sales.cs_coupon_amt,
	catalog_sales.cs_list_price,
	catalog_sales.cs_net_profit,
	catalog_sales.cs_ship_addr_sk,
	inventory.inv_item_sk,
	catalog_sales.cs_net_paid_inc_tax,
	catalog_sales.cs_ext_discount_amt,
	catalog_sales.cs_sold_date_sk
FROM
	inventory,
	catalog_sales
WHERE
	catalog_sales.cs_item_sk = inventory.inv_item_sk
DISTRIBUTE BY cs_sold_date_sk;