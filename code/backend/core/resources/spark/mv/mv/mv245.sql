CREATE MATERIALIZED VIEW IF NOT EXISTS mv245
AS
SELECT
	customer_demographics.cd_purchase_estimate,
	catalog_returns.cr_item_sk,
	catalog_sales.cs_ship_customer_sk,
	store_sales.ss_hdemo_sk,
	catalog_sales.cs_catalog_page_sk,
	customer.c_birth_month,
	date_dim.d_qoy,
	store.s_store_name,
	catalog_sales.cs_call_center_sk,
	customer_demographics.cd_dep_count,
	catalog_returns.cr_reversed_charge,
	store_sales.ss_list_price,
	store_sales.ss_store_sk,
	catalog_sales.cs_bill_cdemo_sk,
	store_sales.ss_ext_wholesale_cost,
	catalog_returns.cr_order_number,
	customer.c_birth_country,
	catalog_sales.cs_sold_date_sk,
	customer.c_last_review_date_sk,
	catalog_sales.cs_bill_addr_sk,
	store.s_street_name,
	customer_demographics.cd_demo_sk,
	store_returns.sr_reason_sk,
	store.s_market_id,
	date_dim.d_moy,
	store_sales.ss_customer_sk,
	date_dim.d_week_seq,
	store_returns.sr_return_quantity,
	customer.c_current_cdemo_sk,
	catalog_sales.cs_item_sk,
	store_returns.sr_cdemo_sk,
	catalog_sales.cs_bill_customer_sk,
	customer.c_last_name,
	store.s_state,
	customer.c_birth_day,
	customer.c_email_address,
	catalog_returns.cr_store_credit,
	store_returns.sr_customer_sk,
	customer_demographics.cd_gender,
	date_dim.d_quarter_name,
	store_sales.ss_sales_price,
	customer.c_birth_year,
	catalog_returns.cr_call_center_sk,
	customer_demographics.cd_dep_employed_count,
	store_returns.sr_net_loss,
	store.s_store_sk,
	catalog_sales.cs_sold_time_sk,
	catalog_returns.cr_refunded_cash,
	store_sales.ss_ext_discount_amt,
	store_sales.ss_promo_sk,
	catalog_sales.cs_sales_price,
	store_sales.ss_wholesale_cost,
	catalog_returns.cr_returning_customer_sk,
	catalog_sales.cs_quantity,
	catalog_sales.cs_coupon_amt,
	catalog_returns.cr_returned_date_sk,
	store.s_number_employees,
	catalog_returns.cr_catalog_page_sk,
	date_dim.d_dom,
	store.s_county,
	store.s_street_type,
	catalog_sales.cs_net_paid_inc_tax,
	catalog_sales.cs_ext_discount_amt,
	store_sales.ss_net_paid,
	customer.c_preferred_cust_flag,
	store_sales.ss_quantity,
	date_dim.d_month_seq,
	store.s_suite_number,
	store_sales.ss_ext_sales_price,
	customer_demographics.cd_dep_college_count,
	catalog_sales.cs_ext_ship_cost,
	date_dim.d_date_sk,
	store_sales.ss_ticket_number,
	store.s_zip,
	store_sales.ss_cdemo_sk,
	date_dim.d_date,
	store_returns.sr_ticket_number,
	store.s_store_id,
	catalog_sales.cs_order_number,
	catalog_sales.cs_wholesale_cost,
	catalog_sales.cs_ext_sales_price,
	customer_demographics.cd_education_status,
	customer.c_current_addr_sk,
	customer.c_first_name,
	store.s_company_name,
	catalog_sales.cs_net_paid,
	store.s_city,
	catalog_returns.cr_returning_addr_sk,
	catalog_returns.cr_net_loss,
	catalog_sales.cs_ext_wholesale_cost,
	catalog_sales.cs_ship_date_sk,
	store_sales.ss_ext_list_price,
	store.s_gmt_offset,
	date_dim.d_year,
	catalog_sales.cs_promo_sk,
	catalog_sales.cs_ship_mode_sk,
	catalog_sales.cs_ext_list_price,
	store_sales.ss_net_profit,
	store_sales.ss_sold_date_sk,
	catalog_returns.cr_return_quantity,
	customer.c_first_shipto_date_sk,
	store_sales.ss_addr_sk,
	catalog_returns.cr_return_amount,
	catalog_sales.cs_bill_hdemo_sk,
	store.s_company_id,
	store_sales.ss_item_sk,
	catalog_returns.cr_return_amt_inc_tax,
	customer.c_customer_sk,
	store_sales.ss_coupon_amt,
	customer_demographics.cd_marital_status,
	catalog_sales.cs_warehouse_sk,
	store_sales.ss_sold_time_sk,
	customer_demographics.cd_credit_rating,
	store_returns.sr_returned_date_sk,
	customer.c_customer_id,
	customer.c_login,
	catalog_sales.cs_list_price,
	store_returns.sr_item_sk,
	catalog_sales.cs_net_profit,
	catalog_sales.cs_ship_addr_sk,
	customer.c_first_sales_date_sk,
	store_returns.sr_return_amt,
	store.s_street_number,
	date_dim.d_day_name,
	store_sales.ss_ext_tax,
	customer.c_salutation,
	store_returns.sr_store_sk,
	date_dim.d_dow,
	customer.c_current_hdemo_sk
FROM
	date_dim,
	store_sales,
	store_returns,
	store,
	customer,
	catalog_returns,
	catalog_sales,
	customer_demographics
WHERE
	store_sales.ss_store_sk = store.s_store_sk
	AND store_sales.ss_customer_sk = customer.c_customer_sk
	AND store_sales.ss_ticket_number = store_returns.sr_ticket_number
	AND store_sales.ss_item_sk = catalog_sales.cs_item_sk
	AND store_sales.ss_item_sk = store_returns.sr_item_sk
	AND store_sales.ss_sold_date_sk = date_dim.d_date_sk
	AND store_sales.ss_cdemo_sk = customer_demographics.cd_demo_sk
	AND customer.c_first_shipto_date_sk = date_dim.d_date_sk
	AND customer.c_first_sales_date_sk = date_dim.d_date_sk
	AND catalog_returns.cr_refunded_cash > catalog_returns.cr_refunded_cash
	AND catalog_sales.cs_order_number = catalog_returns.cr_order_number
	AND catalog_sales.cs_item_sk = catalog_returns.cr_item_sk
	AND (date_dim.d_year = 1999 OR date_dim.d_year = 2000);