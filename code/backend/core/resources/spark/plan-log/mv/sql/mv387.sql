SELECT
	warehouse.w_state,
	catalog_sales.cs_ext_ship_cost,
	catalog_sales.cs_wholesale_cost,
	catalog_sales.cs_quantity,
	ship_mode.sm_carrier,
	catalog_sales.cs_net_profit,
	catalog_sales.cs_net_paid,
	catalog_sales.cs_sold_time_sk,
	catalog_sales.cs_item_sk,
	warehouse.w_warehouse_sk,
	catalog_sales.cs_list_price,
	warehouse.w_county,
	catalog_sales.cs_order_number,
	ship_mode.sm_type,
	catalog_sales.cs_ext_wholesale_cost,
	warehouse.w_warehouse_name,
	call_center.cc_call_center_sk,
	catalog_sales.cs_ext_sales_price,
	catalog_sales.cs_catalog_page_sk,
	catalog_sales.cs_call_center_sk,
	catalog_sales.cs_sales_price,
	catalog_sales.cs_ship_addr_sk,
	warehouse.w_city,
	catalog_sales.cs_net_paid_inc_tax,
	call_center.cc_name,
	call_center.cc_county,
	catalog_sales.cs_bill_cdemo_sk,
	catalog_sales.cs_warehouse_sk,
	catalog_sales.cs_ship_customer_sk,
	call_center.cc_manager,
	catalog_sales.cs_ship_mode_sk,
	warehouse.w_warehouse_sq_ft,
	catalog_sales.cs_sold_date_sk,
	catalog_sales.cs_ext_list_price,
	catalog_sales.cs_coupon_amt,
	catalog_sales.cs_ship_date_sk,
	catalog_sales.cs_bill_customer_sk,
	ship_mode.sm_ship_mode_sk,
	catalog_sales.cs_ext_discount_amt,
	catalog_sales.cs_promo_sk,
	catalog_sales.cs_bill_addr_sk,
	call_center.cc_call_center_id,
	catalog_sales.cs_bill_hdemo_sk,
	warehouse.w_country
FROM
	call_center,
	catalog_sales,
	ship_mode,
	warehouse
WHERE
	catalog_sales.cs_warehouse_sk = warehouse.w_warehouse_sk
	AND catalog_sales.cs_call_center_sk = call_center.cc_call_center_sk
	AND catalog_sales.cs_ship_mode_sk = ship_mode.sm_ship_mode_sk