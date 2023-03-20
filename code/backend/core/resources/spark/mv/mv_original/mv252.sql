SELECT
	customer_demographics.cd_purchase_estimate,
	catalog_sales.cs_ship_customer_sk,
	store_sales.ss_hdemo_sk,
	catalog_sales.cs_catalog_page_sk,
	customer.c_birth_month,
	store_sales.ss_list_price,
	income_band.ib_lower_bound,
	customer_address.ca_street_name,
	store_sales.ss_ext_wholesale_cost,
	catalog_returns.cr_order_number,
	customer.c_birth_country,
	customer.c_last_review_date_sk,
	catalog_sales.cs_bill_addr_sk,
	store.s_street_name,
	customer_demographics.cd_demo_sk,
	store_returns.sr_reason_sk,
	date_dim.d_week_seq,
	household_demographics.hd_dep_count,
	customer.c_current_cdemo_sk,
	catalog_sales.cs_bill_customer_sk,
	customer.c_email_address,
	date_dim.d_quarter_name,
	customer_address.ca_street_number,
	income_band.ib_income_band_sk,
	customer_demographics.cd_dep_employed_count,
	store_returns.sr_net_loss,
	catalog_sales.cs_sold_time_sk,
	store_sales.ss_promo_sk,
	catalog_sales.cs_sales_price,
	store_sales.ss_wholesale_cost,
	catalog_returns.cr_returning_customer_sk,
	customer_address.ca_country,
	catalog_sales.cs_quantity,
	catalog_returns.cr_returned_date_sk,
	catalog_sales.cs_coupon_amt,
	date_dim.d_dom,
	store.s_county,
	store.s_street_type,
	customer.c_preferred_cust_flag,
	store.s_suite_number,
	customer_address.ca_street_type,
	income_band.ib_upper_bound,
	customer_demographics.cd_dep_college_count,
	household_demographics.hd_buy_potential,
	store_returns.sr_ticket_number,
	store.s_store_id,
	catalog_sales.cs_order_number,
	catalog_sales.cs_ext_sales_price,
	customer_demographics.cd_education_status,
	store.s_company_name,
	catalog_sales.cs_net_paid,
	store.s_city,
	catalog_returns.cr_returning_addr_sk,
	catalog_sales.cs_ext_wholesale_cost,
	date_dim.d_year,
	customer_address.ca_address_sk,
	catalog_sales.cs_ship_mode_sk,
	catalog_sales.cs_ext_list_price,
	store_sales.ss_sold_date_sk,
	promotion.p_promo_sk,
	catalog_returns.cr_return_quantity,
	customer.c_first_shipto_date_sk,
	store_sales.ss_addr_sk,
	catalog_returns.cr_return_amount,
	catalog_sales.cs_bill_hdemo_sk,
	store.s_company_id,
	store_sales.ss_item_sk,
	catalog_returns.cr_return_amt_inc_tax,
	customer.c_customer_sk,
	catalog_sales.cs_warehouse_sk,
	customer_demographics.cd_credit_rating,
	store_returns.sr_returned_date_sk,
	catalog_sales.cs_list_price,
	store_returns.sr_item_sk,
	store_returns.sr_return_amt,
	store.s_street_number,
	store_sales.ss_ext_tax,
	customer.c_salutation,
	store_returns.sr_store_sk,
	customer.c_current_hdemo_sk,
	catalog_returns.cr_item_sk,
	promotion.p_channel_tv,
	household_demographics.hd_vehicle_count,
	date_dim.d_qoy,
	store.s_store_name,
	catalog_sales.cs_call_center_sk,
	customer_demographics.cd_dep_count,
	catalog_returns.cr_reversed_charge,
	customer_address.ca_city,
	store_sales.ss_store_sk,
	catalog_sales.cs_bill_cdemo_sk,
	promotion.p_channel_event,
	store.s_market_id,
	date_dim.d_moy,
	store_sales.ss_customer_sk,
	store_returns.sr_return_quantity,
	catalog_sales.cs_item_sk,
	store_returns.sr_cdemo_sk,
	customer.c_last_name,
	store.s_state,
	customer.c_birth_day,
	catalog_returns.cr_store_credit,
	household_demographics.hd_demo_sk,
	store_returns.sr_customer_sk,
	customer_demographics.cd_gender,
	store_sales.ss_sales_price,
	customer.c_birth_year,
	customer_address.ca_state,
	catalog_returns.cr_call_center_sk,
	store.s_store_sk,
	catalog_returns.cr_refunded_cash,
	household_demographics.hd_income_band_sk,
	store_sales.ss_ext_discount_amt,
	store.s_number_employees,
	customer_address.ca_suite_number,
	catalog_returns.cr_catalog_page_sk,
	catalog_sales.cs_net_paid_inc_tax,
	catalog_sales.cs_ext_discount_amt,
	store_sales.ss_net_paid,
	store_sales.ss_quantity,
	promotion.p_channel_dmail,
	customer_address.ca_location_type,
	date_dim.d_month_seq,
	store_sales.ss_ext_sales_price,
	catalog_sales.cs_ext_ship_cost,
	date_dim.d_date_sk,
	customer_address.ca_county,
	store_sales.ss_ticket_number,
	promotion.p_channel_email,
	store.s_zip,
	store_sales.ss_cdemo_sk,
	date_dim.d_date,
	catalog_sales.cs_wholesale_cost,
	customer.c_current_addr_sk,
	customer.c_first_name,
	customer_address.ca_zip,
	catalog_returns.cr_net_loss,
	catalog_sales.cs_ship_date_sk,
	store_sales.ss_ext_list_price,
	store.s_gmt_offset,
	catalog_sales.cs_promo_sk,
	store_sales.ss_net_profit,
	store_sales.ss_coupon_amt,
	customer_demographics.cd_marital_status,
	store_sales.ss_sold_time_sk,
	customer_address.ca_gmt_offset,
	customer.c_customer_id,
	customer.c_login,
	catalog_sales.cs_net_profit,
	catalog_sales.cs_ship_addr_sk,
	customer.c_first_sales_date_sk,
	date_dim.d_day_name,
	date_dim.d_dow,
	catalog_sales.cs_sold_date_sk
FROM
	store_returns,
	customer,
	promotion,
	income_band,
	date_dim,
	store_sales,
	store,
	customer_address,
	catalog_returns,
	catalog_sales,
	customer_demographics,
	household_demographics
WHERE
	store_sales.ss_store_sk = store.s_store_sk
	AND store_sales.ss_customer_sk = customer.c_customer_sk
	AND store_sales.ss_item_sk = catalog_sales.cs_item_sk
	AND store_sales.ss_item_sk = store_returns.sr_item_sk
	AND store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
	AND store_sales.ss_cdemo_sk = customer_demographics.cd_demo_sk
	AND store_sales.ss_ticket_number = store_returns.sr_ticket_number
	AND store_sales.ss_promo_sk = promotion.p_promo_sk
	AND store_sales.ss_sold_date_sk = date_dim.d_date_sk
	AND store_sales.ss_addr_sk = customer_address.ca_address_sk
	AND customer.c_current_addr_sk = customer_address.ca_address_sk
	AND customer.c_first_sales_date_sk = date_dim.d_date_sk
	AND customer.c_current_hdemo_sk = household_demographics.hd_demo_sk
	AND customer.c_first_shipto_date_sk = date_dim.d_date_sk
	AND customer.c_current_cdemo_sk = customer_demographics.cd_demo_sk
	AND catalog_returns.cr_refunded_cash > catalog_returns.cr_refunded_cash
	AND catalog_sales.cs_item_sk = catalog_returns.cr_item_sk
	AND catalog_sales.cs_order_number = catalog_returns.cr_order_number
	AND household_demographics.hd_income_band_sk = income_band.ib_income_band_sk
	AND (date_dim.d_year = 1999 OR date_dim.d_year = 2000)