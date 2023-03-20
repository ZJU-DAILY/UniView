CREATE MATERIALIZED VIEW IF NOT EXISTS mv69
PARTITIONED BY (cs_sold_date_sk)
AS
SELECT
	customer_address.ca_location_type,
	date_dim.d_month_seq,
	catalog_sales.cs_ship_customer_sk,
	customer_address.ca_street_type,
	catalog_sales.cs_ext_ship_cost,
	date_dim.d_date_sk,
	catalog_sales.cs_catalog_page_sk,
	date_dim.d_qoy,
	catalog_sales.cs_call_center_sk,
	customer_address.ca_county,
	customer_address.ca_city,
	date_dim.d_date,
	customer_address.ca_street_name,
	catalog_sales.cs_order_number,
	catalog_sales.cs_bill_cdemo_sk,
	catalog_sales.cs_ext_sales_price,
	catalog_sales.cs_wholesale_cost,
	catalog_sales.cs_bill_addr_sk,
	catalog_sales.cs_net_paid,
	customer_address.ca_zip,
	catalog_sales.cs_ext_wholesale_cost,
	catalog_sales.cs_ship_date_sk,
	date_dim.d_moy,
	date_dim.d_week_seq,
	date_dim.d_year,
	customer_address.ca_address_sk,
	catalog_sales.cs_promo_sk,
	catalog_sales.cs_item_sk,
	catalog_sales.cs_ship_mode_sk,
	catalog_sales.cs_bill_customer_sk,
	catalog_sales.cs_ext_list_price,
	date_dim.d_quarter_name,
	catalog_sales.cs_bill_hdemo_sk,
	customer_address.ca_street_number,
	catalog_sales.cs_warehouse_sk,
	customer_address.ca_state,
	customer_address.ca_gmt_offset,
	catalog_sales.cs_sold_time_sk,
	catalog_sales.cs_sales_price,
	catalog_sales.cs_quantity,
	catalog_sales.cs_coupon_amt,
	catalog_sales.cs_list_price,
	customer_address.ca_country,
	catalog_sales.cs_net_profit,
	catalog_sales.cs_ship_addr_sk,
	customer_address.ca_suite_number,
	date_dim.d_day_name,
	date_dim.d_dom,
	catalog_sales.cs_net_paid_inc_tax,
	catalog_sales.cs_ext_discount_amt,
	date_dim.d_dow,
	catalog_sales.cs_sold_date_sk
FROM
	catalog_sales ANTI JOIN catalog_returns ON (catalog_sales.cs_order_number = catalog_returns.cr_order_number)	Join date_dim
	Join customer_address
WHERE
	catalog_sales.cs_ship_addr_sk = customer_address.ca_address_sk
	AND catalog_sales.cs_ship_date_sk = date_dim.d_date_sk
	AND customer_address.ca_state = 'GA'
	AND (((date_dim.d_date >= cast('1970-01-01' as date) + interval 11719 days) AND (date_dim.d_date <= cast('1970-01-01' as date) + interval 11779 days)))
DISTRIBUTE BY cs_sold_date_sk;