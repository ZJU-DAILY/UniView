SELECT
	item.i_manufact,
	store_sales.ss_promo_sk,
	date_dim.d_quarter_name,
	date_dim.d_day_name,
	store_sales.ss_store_sk,
	customer_demographics.cd_education_status,
	item.i_manager_id,
	store_sales.ss_customer_sk,
	store.s_county,
	item.i_class_id,
	customer_demographics.cd_dep_employed_count,
	date_dim.d_qoy,
	customer_demographics.cd_demo_sk,
	store_sales.ss_sold_date_sk,
	store_sales.ss_cdemo_sk,
	customer_demographics.cd_credit_rating,
	store.s_store_sk,
	date_dim.d_week_seq,
	date_dim.d_year,
	item.i_item_sk,
	item.i_product_name,
	store_sales.ss_ext_sales_price,
	item.i_wholesale_cost,
	store_sales.ss_ext_tax,
	store.s_number_employees,
	store.s_store_id,
	customer_demographics.cd_purchase_estimate,
	store_sales.ss_ext_wholesale_cost,
	customer_demographics.cd_marital_status,
	store_sales.ss_ext_list_price,
	item.i_class,
	store_sales.ss_sold_time_sk,
	store.s_city,
	item.i_category,
	customer_demographics.cd_gender,
	store_sales.ss_hdemo_sk,
	date_dim.d_date_sk,
	item.i_category_id,
	date_dim.d_dow,
	store.s_zip,
	store_sales.ss_ext_discount_amt,
	store.s_street_type,
	store_sales.ss_item_sk,
	date_dim.d_dom,
	item.i_color,
	customer_demographics.cd_dep_college_count,
	store.s_street_name,
	store_sales.ss_coupon_amt,
	item.i_size,
	store.s_gmt_offset,
	store_sales.ss_addr_sk,
	item.i_units,
	store.s_store_name,
	store_sales.ss_net_paid,
	item.i_brand_id,
	store.s_market_id,
	item.i_brand,
	store_sales.ss_list_price,
	store_sales.ss_ticket_number,
	item.i_current_price,
	date_dim.d_month_seq,
	item.i_item_id,
	store_sales.ss_quantity,
	store.s_company_id,
	store.s_company_name,
	store.s_state,
	item.i_manufact_id,
	store.s_suite_number,
	customer_demographics.cd_dep_count,
	store_sales.ss_sales_price,
	item.i_item_desc,
	date_dim.d_moy,
	date_dim.d_date,
	store_sales.ss_net_profit,
	store_sales.ss_wholesale_cost,
	store.s_street_number
FROM
	item,
	store,
	customer_demographics,
	date_dim,
	store_sales
WHERE
	customer_demographics.cd_gender = 'M'
	AND customer_demographics.cd_marital_status = 'S'
	AND customer_demographics.cd_education_status = 'College'
	AND store_sales.ss_cdemo_sk = customer_demographics.cd_demo_sk
	AND date_dim.d_year = 2002
	AND store_sales.ss_sold_date_sk = date_dim.d_date_sk
	AND store_sales.ss_item_sk = item.i_item_sk
	AND store_sales.ss_store_sk = store.s_store_sk
	AND store.s_state = 'TN'