CREATE MATERIALIZED VIEW IF NOT EXISTS mv135
PARTITIONED BY (ss_sold_date_sk)
AS
SELECT
	customer_address.ca_location_type,
	date_dim.d_month_seq,
	store_sales.ss_hdemo_sk,
	store_sales.ss_ext_sales_price,
	customer_address.ca_street_type,
	date_dim.d_date_sk,
	date_dim.d_qoy,
	customer_address.ca_county,
	store_sales.ss_ticket_number,
	customer_address.ca_city,
	store_sales.ss_cdemo_sk,
	date_dim.d_date,
	store_sales.ss_list_price,
	store_sales.ss_store_sk,
	customer_address.ca_street_name,
	store_sales.ss_ext_wholesale_cost,
	customer_address.ca_zip,
	date_dim.d_moy,
	store_sales.ss_customer_sk,
	date_dim.d_week_seq,
	store_sales.ss_ext_list_price,
	date_dim.d_year,
	customer_address.ca_address_sk,
	store_sales.ss_net_profit,
	store_sales.ss_sold_date_sk,
	store_sales.ss_addr_sk,
	date_dim.d_quarter_name,
	store_sales.ss_sales_price,
	store_sales.ss_item_sk,
	store_sales.ss_coupon_amt,
	customer_address.ca_street_number,
	store_sales.ss_sold_time_sk,
	customer_address.ca_state,
	customer_address.ca_gmt_offset,
	store_sales.ss_ext_discount_amt,
	store_sales.ss_promo_sk,
	store_sales.ss_wholesale_cost,
	customer_address.ca_country,
	customer_address.ca_suite_number,
	date_dim.d_day_name,
	date_dim.d_dom,
	store_sales.ss_ext_tax,
	store_sales.ss_net_paid,
	store_sales.ss_quantity,
	date_dim.d_dow
FROM
	date_dim,
	store_sales,
	customer_address
WHERE
	store_sales.ss_sold_date_sk = date_dim.d_date_sk
	AND store_sales.ss_addr_sk = customer_address.ca_address_sk
	AND (date_dim.d_year = 1998 OR date_dim.d_year = 2001)
	AND (date_dim.d_moy = 5 OR date_dim.d_moy = 2 OR date_dim.d_moy = 9)
DISTRIBUTE BY ss_sold_date_sk;