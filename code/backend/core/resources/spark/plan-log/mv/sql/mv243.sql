SELECT
	catalog_sales.cs_ext_ship_cost,
	store_returns.sr_returned_date_sk,
	store_sales.ss_promo_sk,
	store_returns.sr_return_amt,
	date_dim.d_quarter_name,
	date_dim.d_day_name,
	store_sales.ss_store_sk,
	catalog_sales.cs_bill_addr_sk,
	catalog_sales.cs_net_profit,
	catalog_sales.cs_net_paid,
	store_sales.ss_customer_sk,
	catalog_sales.cs_item_sk,
	catalog_sales.cs_list_price,
	date_dim.d_qoy,
	catalog_returns.cr_returned_date_sk,
	store.s_county,
	catalog_sales.cs_order_number,
	catalog_returns.cr_call_center_sk,
	store_sales.ss_sold_date_sk,
	customer.c_first_sales_date_sk,
	customer.c_first_name,
	store_sales.ss_cdemo_sk,
	catalog_sales.cs_ext_wholesale_cost,
	customer.c_last_name,
	store.s_store_sk,
	catalog_returns.cr_returning_addr_sk,
	date_dim.d_week_seq,
	date_dim.d_year,
	store_sales.ss_ext_sales_price,
	catalog_returns.cr_return_amount,
	store_returns.sr_item_sk,
	store_sales.ss_ext_tax,
	store.s_number_employees,
	store.s_store_id,
	store_sales.ss_ext_wholesale_cost,
	customer.c_current_cdemo_sk,
	catalog_returns.cr_item_sk,
	store_returns.sr_store_sk,
	catalog_returns.cr_refunded_cash,
	store_sales.ss_ext_list_price,
	customer.c_customer_sk,
	catalog_returns.cr_return_quantity,
	catalog_sales.cs_sold_date_sk,
	store_sales.ss_sold_time_sk,
	store_returns.sr_ticket_number,
	customer.c_current_hdemo_sk,
	customer.c_birth_day,
	catalog_sales.cs_coupon_amt,
	catalog_sales.cs_ship_date_sk,
	customer.c_login,
	catalog_sales.cs_bill_customer_sk,
	customer.c_salutation,
	catalog_sales.cs_ext_discount_amt,
	store_sales.ss_hdemo_sk,
	store.s_city,
	catalog_sales.cs_bill_hdemo_sk,
	date_dim.d_date_sk,
	customer.c_first_shipto_date_sk,
	date_dim.d_dow,
	customer.c_birth_year,
	customer.c_current_addr_sk,
	catalog_sales.cs_wholesale_cost,
	catalog_sales.cs_quantity,
	store.s_zip,
	store_sales.ss_ext_discount_amt,
	customer.c_birth_country,
	store.s_street_type,
	store_sales.ss_item_sk,
	store_returns.sr_net_loss,
	catalog_sales.cs_sold_time_sk,
	customer.c_birth_month,
	customer.c_customer_id,
	date_dim.d_dom,
	store_returns.sr_cdemo_sk,
	catalog_returns.cr_order_number,
	catalog_returns.cr_return_amt_inc_tax,
	store.s_street_name,
	store_sales.ss_coupon_amt,
	store.s_gmt_offset,
	catalog_sales.cs_ext_sales_price,
	catalog_sales.cs_catalog_page_sk,
	store_sales.ss_addr_sk,
	catalog_sales.cs_call_center_sk,
	store.s_store_name,
	store_returns.sr_customer_sk,
	catalog_sales.cs_sales_price,
	catalog_sales.cs_ship_addr_sk,
	catalog_returns.cr_store_credit,
	catalog_sales.cs_net_paid_inc_tax,
	catalog_returns.cr_returning_customer_sk,
	store.s_market_id,
	store_sales.ss_list_price,
	store_sales.ss_ticket_number,
	store_returns.sr_return_quantity,
	date_dim.d_month_seq,
	store_sales.ss_quantity,
	store.s_company_id,
	catalog_sales.cs_bill_cdemo_sk,
	customer.c_last_review_date_sk,
	catalog_sales.cs_warehouse_sk,
	store.s_company_name,
	catalog_sales.cs_ship_customer_sk,
	store.s_state,
	store.s_suite_number,
	store_returns.sr_reason_sk,
	catalog_sales.cs_ship_mode_sk,
	customer.c_email_address,
	catalog_returns.cr_net_loss,
	store_sales.ss_sales_price,
	catalog_sales.cs_ext_list_price,
	date_dim.d_moy,
	date_dim.d_date,
	store_sales.ss_net_profit,
	catalog_sales.cs_promo_sk,
	catalog_returns.cr_catalog_page_sk,
	customer.c_preferred_cust_flag,
	store_sales.ss_wholesale_cost,
	store.s_street_number,
	catalog_returns.cr_reversed_charge,
	store_sales.ss_net_paid
FROM
	customer,
	store_returns,
	date_dim,
	catalog_sales,
	store_sales,
	catalog_returns,
	store
WHERE
	customer.c_first_sales_date_sk = date_dim.d_date_sk
	AND store_sales.ss_customer_sk = customer.c_customer_sk
	AND customer.c_first_shipto_date_sk = date_dim.d_date_sk
	AND store_sales.ss_sold_date_sk = date_dim.d_date_sk
	AND (date_dim.d_year = 1999 OR date_dim.d_year = 2000)
	AND store_sales.ss_item_sk = catalog_sales.cs_item_sk
	AND store_sales.ss_item_sk = store_returns.sr_item_sk
	AND store_sales.ss_ticket_number = store_returns.sr_ticket_number
	AND store_sales.ss_store_sk = store.s_store_sk
	AND catalog_sales.cs_item_sk = catalog_returns.cr_item_sk
	AND catalog_sales.cs_order_number = catalog_returns.cr_order_number
	AND catalog_returns.cr_refunded_cash > catalog_returns.cr_refunded_cash