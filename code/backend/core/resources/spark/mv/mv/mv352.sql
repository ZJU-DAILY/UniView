CREATE MATERIALIZED VIEW IF NOT EXISTS mv352
AS
SELECT
	web_returns.wr_returning_customer_sk,
	customer_demographics.cd_purchase_estimate,
	web_sales.ws_ship_mode_sk,
	web_returns.wr_item_sk,
	web_sales.ws_order_number,
	date_dim.d_qoy,
	web_returns.wr_refunded_cdemo_sk,
	customer_demographics.cd_dep_count,
	web_sales.ws_ship_hdemo_sk,
	customer_address.ca_city,
	web_returns.wr_refunded_addr_sk,
	web_sales.ws_web_site_sk,
	web_sales.ws_sold_time_sk,
	customer_address.ca_street_name,
	reason.r_reason_desc,
	web_sales.ws_sales_price,
	customer_demographics.cd_demo_sk,
	date_dim.d_moy,
	web_sales.ws_bill_addr_sk,
	web_returns.wr_returning_addr_sk,
	date_dim.d_week_seq,
	web_sales.ws_ext_ship_cost,
	web_returns.wr_fee,
	web_sales.ws_ship_customer_sk,
	customer_demographics.cd_gender,
	web_returns.wr_return_amt,
	date_dim.d_quarter_name,
	web_sales.ws_wholesale_cost,
	web_returns.wr_net_loss,
	web_returns.wr_web_page_sk,
	customer_address.ca_street_number,
	web_returns.wr_reason_sk,
	customer_address.ca_state,
	customer_demographics.cd_dep_employed_count,
	web_sales.ws_ext_sales_price,
	web_returns.wr_returned_date_sk,
	customer_address.ca_country,
	web_returns.wr_returning_cdemo_sk,
	customer_address.ca_suite_number,
	date_dim.d_dom,
	reason.r_reason_sk,
	customer_address.ca_location_type,
	date_dim.d_month_seq,
	web_sales.ws_ship_date_sk,
	customer_address.ca_street_type,
	customer_demographics.cd_dep_college_count,
	web_page.wp_char_count,
	date_dim.d_date_sk,
	web_sales.ws_promo_sk,
	customer_address.ca_county,
	date_dim.d_date,
	web_page.wp_web_page_sk,
	customer_demographics.cd_education_status,
	web_sales.ws_ext_list_price,
	customer_address.ca_zip,
	web_sales.ws_warehouse_sk,
	web_sales.ws_quantity,
	web_returns.wr_return_quantity,
	web_sales.ws_web_page_sk,
	date_dim.d_year,
	customer_address.ca_address_sk,
	web_sales.ws_net_paid,
	web_returns.wr_refunded_cash,
	web_sales.ws_ext_discount_amt,
	web_sales.ws_ext_wholesale_cost,
	customer_demographics.cd_marital_status,
	web_sales.ws_list_price,
	customer_demographics.cd_credit_rating,
	web_sales.ws_sold_date_sk,
	customer_address.ca_gmt_offset,
	web_sales.ws_ship_addr_sk,
	web_returns.wr_order_number,
	date_dim.d_day_name,
	web_sales.ws_bill_customer_sk,
	web_sales.ws_net_profit,
	web_sales.ws_item_sk,
	date_dim.d_dow
FROM
	date_dim,
	web_sales,
	web_returns,
	customer_address,
	reason,
	customer_demographics,
	web_page
WHERE
	(((customer_demographics.cd_marital_status = 'M') OR (customer_demographics.cd_marital_status = 'S') OR (customer_demographics.cd_marital_status = 'W')))
	AND (((customer_demographics.cd_education_status = 'Advanced Degree') OR (customer_demographics.cd_education_status = 'College') OR (customer_demographics.cd_education_status = '2 yr Degree')))
	AND web_returns.wr_refunded_cdemo_sk = customer_demographics.cd_demo_sk
	AND web_returns.wr_returning_cdemo_sk = customer_demographics.cd_demo_sk
	AND web_returns.wr_refunded_addr_sk = customer_address.ca_address_sk
	AND web_sales.ws_order_number = web_returns.wr_order_number
	AND web_sales.ws_item_sk = web_returns.wr_item_sk
	AND web_returns.wr_reason_sk = reason.r_reason_sk
	AND customer_address.ca_country = 'United States'
	AND (((customer_address.ca_state = 'IN') OR (customer_address.ca_state = 'OH') OR (customer_address.ca_state = 'NJ') OR (customer_address.ca_state = 'WI') OR (customer_address.ca_state = 'CT') OR (customer_address.ca_state = 'KY') OR (customer_address.ca_state = 'LA') OR (customer_address.ca_state = 'IA') OR (customer_address.ca_state = 'AR')))
	AND web_sales.ws_sold_date_sk = date_dim.d_date_sk
	AND web_sales.ws_web_page_sk = web_page.wp_web_page_sk
	AND date_dim.d_year = 2000;