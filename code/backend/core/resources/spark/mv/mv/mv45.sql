CREATE MATERIALIZED VIEW IF NOT EXISTS mv45
PARTITIONED BY (ss_sold_date_sk)
AS
SELECT
	store.s_suite_number,
	store_sales.ss_hdemo_sk,
	store_sales.ss_ext_sales_price,
	store.s_store_name,
	store_sales.ss_ticket_number,
	store.s_zip,
	store_sales.ss_cdemo_sk,
	store_sales.ss_list_price,
	store.s_store_id,
	store_sales.ss_store_sk,
	store_sales.ss_ext_wholesale_cost,
	store.s_company_name,
	store.s_street_name,
	store.s_city,
	store.s_market_id,
	store_sales.ss_customer_sk,
	store_sales.ss_ext_list_price,
	store.s_gmt_offset,
	store.s_state,
	store_sales.ss_net_profit,
	store_sales.ss_sold_date_sk,
	store_sales.ss_addr_sk,
	store_sales.ss_sales_price,
	store_sales.ss_item_sk,
	store_sales.ss_coupon_amt,
	store.s_company_id,
	store_sales.ss_sold_time_sk,
	store.s_store_sk,
	store_sales.ss_ext_discount_amt,
	store_sales.ss_promo_sk,
	store_sales.ss_wholesale_cost,
	store.s_number_employees,
	store.s_street_number,
	store.s_county,
	store_sales.ss_ext_tax,
	store.s_street_type,
	store_sales.ss_net_paid,
	store_sales.ss_quantity
FROM
	store_sales,
	store
WHERE
	store_sales.ss_store_sk = store.s_store_sk
DISTRIBUTE BY ss_sold_date_sk;