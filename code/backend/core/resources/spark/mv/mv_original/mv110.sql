SELECT
	item.i_brand,
	store.s_suite_number,
	store_sales.ss_hdemo_sk,
	store_sales.ss_ext_sales_price,
	item.i_brand_id,
	store.s_store_name,
	store_sales.ss_ticket_number,
	store.s_zip,
	store_sales.ss_cdemo_sk,
	store_sales.ss_list_price,
	store_returns.sr_ticket_number,
	item.i_manager_id,
	store.s_store_id,
	store_sales.ss_store_sk,
	item.i_class_id,
	item.i_item_desc,
	item.i_class,
	store_sales.ss_ext_wholesale_cost,
	item.i_category,
	store.s_company_name,
	store.s_street_name,
	item.i_size,
	store.s_city,
	store_returns.sr_reason_sk,
	store.s_market_id,
	item.i_item_id,
	item.i_current_price,
	store_sales.ss_ext_list_price,
	store_sales.ss_customer_sk,
	store.s_gmt_offset,
	store_returns.sr_return_quantity,
	store_returns.sr_cdemo_sk,
	store.s_state,
	item.i_units,
	store_sales.ss_net_profit,
	item.i_manufact,
	store_sales.ss_sold_date_sk,
	store_returns.sr_customer_sk,
	store_sales.ss_addr_sk,
	store_sales.ss_sales_price,
	store_sales.ss_item_sk,
	store_sales.ss_coupon_amt,
	store.s_company_id,
	item.i_item_sk,
	store_sales.ss_sold_time_sk,
	item.i_product_name,
	item.i_wholesale_cost,
	store_returns.sr_net_loss,
	store.s_store_sk,
	store_returns.sr_returned_date_sk,
	store_sales.ss_ext_discount_amt,
	item.i_color,
	store_sales.ss_promo_sk,
	store_sales.ss_wholesale_cost,
	store_returns.sr_item_sk,
	store.s_number_employees,
	store.s_street_number,
	store_returns.sr_return_amt,
	item.i_category_id,
	store.s_county,
	store_sales.ss_ext_tax,
	item.i_manufact_id,
	store_returns.sr_store_sk,
	store.s_street_type,
	store_sales.ss_net_paid,
	store_sales.ss_quantity
FROM
	store_sales,
	store,
	store_returns,
	item
WHERE
	store_sales.ss_store_sk = store.s_store_sk
	AND store_sales.ss_item_sk = item.i_item_sk
	AND store_sales.ss_item_sk = store_returns.sr_item_sk
	AND store_sales.ss_ticket_number = store_returns.sr_ticket_number
	AND store.s_market_id = 8