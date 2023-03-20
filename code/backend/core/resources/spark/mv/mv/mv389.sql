CREATE MATERIALIZED VIEW IF NOT EXISTS mv389
PARTITIONED BY (cs_sold_date_sk)
AS
SELECT
	warehouse.w_city,
	date_dim.d_month_seq,
	catalog_sales.cs_ship_customer_sk,
	ship_mode.sm_ship_mode_sk,
	date_dim.d_date_sk,
	catalog_sales.cs_ext_ship_cost,
	date_dim.d_qoy,
	catalog_sales.cs_catalog_page_sk,
	ship_mode.sm_carrier,
	catalog_sales.cs_call_center_sk,
	warehouse.w_warehouse_sq_ft,
	date_dim.d_date,
	warehouse.w_county,
	call_center.cc_manager,
	catalog_sales.cs_order_number,
	catalog_sales.cs_bill_cdemo_sk,
	ship_mode.sm_type,
	catalog_sales.cs_ext_sales_price,
	catalog_sales.cs_wholesale_cost,
	warehouse.w_warehouse_name,
	call_center.cc_call_center_id,
	catalog_sales.cs_bill_addr_sk,
	catalog_sales.cs_net_paid,
	warehouse.w_country,
	catalog_sales.cs_ext_wholesale_cost,
	catalog_sales.cs_ship_date_sk,
	date_dim.d_moy,
	date_dim.d_week_seq,
	call_center.cc_call_center_sk,
	date_dim.d_year,
	catalog_sales.cs_promo_sk,
	catalog_sales.cs_item_sk,
	catalog_sales.cs_ship_mode_sk,
	catalog_sales.cs_bill_customer_sk,
	catalog_sales.cs_ext_list_price,
	call_center.cc_name,
	date_dim.d_quarter_name,
	catalog_sales.cs_bill_hdemo_sk,
	catalog_sales.cs_warehouse_sk,
	catalog_sales.cs_sold_time_sk,
	catalog_sales.cs_sales_price,
	catalog_sales.cs_quantity,
	warehouse.w_warehouse_sk,
	catalog_sales.cs_coupon_amt,
	catalog_sales.cs_list_price,
	catalog_sales.cs_net_profit,
	catalog_sales.cs_ship_addr_sk,
	call_center.cc_county,
	date_dim.d_day_name,
	date_dim.d_dom,
	catalog_sales.cs_net_paid_inc_tax,
	catalog_sales.cs_ext_discount_amt,
	warehouse.w_state,
	date_dim.d_dow,
	catalog_sales.cs_sold_date_sk
FROM
	date_dim,
	catalog_sales,
	call_center,
	warehouse,
	ship_mode
WHERE
	catalog_sales.cs_ship_date_sk = date_dim.d_date_sk
	AND catalog_sales.cs_warehouse_sk = warehouse.w_warehouse_sk
	AND catalog_sales.cs_call_center_sk = call_center.cc_call_center_sk
	AND catalog_sales.cs_ship_mode_sk = ship_mode.sm_ship_mode_sk
	AND (((date_dim.d_month_seq >= 1200) AND (date_dim.d_month_seq <= 1211)))
DISTRIBUTE BY cs_sold_date_sk;