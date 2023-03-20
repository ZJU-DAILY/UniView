CREATE MATERIALIZED VIEW IF NOT EXISTS mv261
PARTITIONED BY (cs_sold_date_sk)
AS
SELECT
	warehouse.w_city,
	date_dim.d_month_seq,
	catalog_sales.cs_ship_customer_sk,
	catalog_sales.cs_ext_discount_amt,
	ship_mode.sm_ship_mode_sk,
	date_dim.d_date_sk,
	catalog_sales.cs_ext_ship_cost,
	ship_mode.sm_carrier,
	date_dim.d_qoy,
	catalog_sales.cs_catalog_page_sk,
	catalog_sales.cs_call_center_sk,
	time_dim.t_time_sk,
	warehouse.w_warehouse_sq_ft,
	date_dim.d_date,
	warehouse.w_county,
	catalog_sales.cs_order_number,
	catalog_sales.cs_bill_cdemo_sk,
	catalog_sales.cs_ext_sales_price,
	catalog_sales.cs_wholesale_cost,
	ship_mode.sm_type,
	warehouse.w_warehouse_name,
	catalog_sales.cs_bill_addr_sk,
	warehouse.w_country,
	catalog_sales.cs_net_paid,
	catalog_sales.cs_ext_wholesale_cost,
	catalog_sales.cs_ship_date_sk,
	date_dim.d_moy,
	date_dim.d_week_seq,
	date_dim.d_year,
	catalog_sales.cs_promo_sk,
	catalog_sales.cs_item_sk,
	catalog_sales.cs_ship_mode_sk,
	catalog_sales.cs_bill_customer_sk,
	catalog_sales.cs_ext_list_price,
	date_dim.d_quarter_name,
	catalog_sales.cs_bill_hdemo_sk,
	catalog_sales.cs_warehouse_sk,
	time_dim.t_meal_time,
	catalog_sales.cs_sold_time_sk,
	catalog_sales.cs_sales_price,
	catalog_sales.cs_quantity,
	warehouse.w_warehouse_sk,
	time_dim.t_minute,
	catalog_sales.cs_coupon_amt,
	catalog_sales.cs_list_price,
	catalog_sales.cs_net_profit,
	catalog_sales.cs_ship_addr_sk,
	date_dim.d_day_name,
	date_dim.d_dom,
	time_dim.t_hour,
	catalog_sales.cs_net_paid_inc_tax,
	time_dim.t_time,
	warehouse.w_state,
	date_dim.d_dow,
	catalog_sales.cs_sold_date_sk
FROM
	date_dim,
	time_dim,
	catalog_sales,
	warehouse,
	ship_mode
WHERE
	(((time_dim.t_time >= 30838) AND (time_dim.t_time <= 59638)))
	AND catalog_sales.cs_sold_time_sk = time_dim.t_time_sk
	AND (((ship_mode.sm_carrier = 'DHL') OR (ship_mode.sm_carrier = 'BARIAN')))
	AND catalog_sales.cs_ship_mode_sk = ship_mode.sm_ship_mode_sk
	AND catalog_sales.cs_warehouse_sk = warehouse.w_warehouse_sk
	AND catalog_sales.cs_sold_date_sk = date_dim.d_date_sk
	AND date_dim.d_year = 2001
DISTRIBUTE BY cs_sold_date_sk;