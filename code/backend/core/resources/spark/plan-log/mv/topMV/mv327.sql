CREATE MATERIALIZED VIEW IF NOT EXISTS mv327 AS
SELECT
	item.i_manufact,
	store_returns.sr_returned_date_sk,
	web_sales.ws_ship_hdemo_sk,
	store_sales.ss_promo_sk,
	store_returns.sr_return_amt,
	promotion.p_channel_dmail,
	date_dim.d_quarter_name,
	date_dim.d_day_name,
	store_sales.ss_store_sk,
	item.i_manager_id,
	store_sales.ss_customer_sk,
	store.s_county,
	item.i_class_id,
	web_sales.ws_list_price,
	date_dim.d_qoy,
	web_returns.wr_returned_date_sk,
	web_returns.wr_net_loss,
	web_returns.wr_refunded_cash,
	web_returns.wr_refunded_addr_sk,
	web_returns.wr_web_page_sk,
	store_sales.ss_sold_date_sk,
	web_sales.ws_order_number,
	web_sales.ws_web_page_sk,
	store_sales.ss_cdemo_sk,
	web_sales.ws_warehouse_sk,
	store.s_store_sk,
	web_returns.wr_returning_addr_sk,
	promotion.p_channel_tv,
	date_dim.d_week_seq,
	web_sales.ws_ship_date_sk,
	date_dim.d_year,
	item.i_item_sk,
	web_sales.ws_net_profit,
	store_sales.ss_ext_sales_price,
	item.i_product_name,
	web_sales.ws_sold_time_sk,
	store_returns.sr_item_sk,
	item.i_wholesale_cost,
	store_sales.ss_ext_tax,
	store.s_number_employees,
	web_site.web_site_sk,
	web_returns.wr_returning_customer_sk,
	store.s_store_id,
	web_sales.ws_sold_date_sk,
	store_sales.ss_ext_wholesale_cost,
	web_sales.ws_web_site_sk,
	store_returns.sr_store_sk,
	store_sales.ss_ext_list_price,
	store_returns.sr_ticket_number,
	item.i_class,
	store_sales.ss_sold_time_sk,
	store.s_city,
	item.i_category,
	web_sales.ws_ext_sales_price,
	store_sales.ss_hdemo_sk,
	web_sales.ws_ext_discount_amt,
	date_dim.d_date_sk,
	item.i_category_id,
	date_dim.d_dow,
	web_sales.ws_ship_mode_sk,
	web_site.web_site_id,
	store.s_zip,
	web_sales.ws_wholesale_cost,
	web_sales.ws_ext_list_price,
	store_sales.ss_ext_discount_amt,
	web_returns.wr_item_sk,
	web_returns.wr_order_number,
	store.s_street_type,
	store_sales.ss_item_sk,
	store_returns.sr_net_loss,
	web_site.web_name,
	date_dim.d_dom,
	web_sales.ws_ship_customer_sk,
	web_site.web_company_name,
	item.i_color,
	store_returns.sr_cdemo_sk,
	web_returns.wr_return_amt,
	store.s_street_name,
	web_sales.ws_ship_addr_sk,
	web_sales.ws_ext_ship_cost,
	web_returns.wr_returning_cdemo_sk,
	web_sales.ws_bill_addr_sk,
	store.s_gmt_offset,
	item.i_size,
	store_sales.ss_coupon_amt,
	store_sales.ss_addr_sk,
	item.i_units,
	store.s_store_name,
	store_returns.sr_customer_sk,
	store_sales.ss_net_paid,
	web_returns.wr_refunded_cdemo_sk,
	web_sales.ws_bill_customer_sk,
	item.i_brand_id,
	promotion.p_promo_sk,
	store.s_market_id,
	item.i_brand,
	store_sales.ss_ticket_number,
	web_sales.ws_sales_price,
	web_returns.wr_fee,
	store_returns.sr_return_quantity,
	date_dim.d_month_seq,
	item.i_current_price,
	store_sales.ss_list_price,
	item.i_item_id,
	web_sales.ws_ext_wholesale_cost,
	store_sales.ss_quantity,
	store.s_company_id,
	web_sales.ws_item_sk,
	promotion.p_channel_event,
	store.s_company_name,
	store.s_state,
	item.i_manufact_id,
	store.s_suite_number,
	web_sales.ws_net_paid,
	store_returns.sr_reason_sk,
	web_sales.ws_promo_sk,
	promotion.p_channel_email,
	web_returns.wr_return_quantity,
	web_sales.ws_quantity,
	store_sales.ss_sales_price,
	item.i_item_desc,
	date_dim.d_moy,
	date_dim.d_date,
	store_sales.ss_net_profit,
	store_sales.ss_wholesale_cost,
	web_returns.wr_reason_sk,
	store.s_street_number
FROM
	web_site,
	item,
	web_returns,
	web_sales,
	promotion,
	store,
	date_dim,
	store_returns,
	store_sales
WHERE
	web_sales.ws_item_sk = web_returns.wr_item_sk
	AND web_sales.ws_order_number = web_returns.wr_order_number
	AND store_sales.ss_item_sk = store_returns.sr_item_sk
	AND store_sales.ss_ticket_number = store_returns.sr_ticket_number
	AND web_sales.ws_item_sk = item.i_item_sk
	AND web_sales.ws_promo_sk = promotion.p_promo_sk
	AND web_sales.ws_sold_date_sk = date_dim.d_date_sk
	AND web_sales.ws_web_site_sk = web_site.web_site_sk
	AND store_sales.ss_item_sk = item.i_item_sk
	AND store_sales.ss_promo_sk = promotion.p_promo_sk
	AND store_sales.ss_sold_date_sk = date_dim.d_date_sk
	AND store_sales.ss_store_sk = store.s_store_sk
	AND promotion.p_channel_tv = 'N'
	AND (((date_dim.d_date >= cast('1970-01-01' as date) + interval 11192 days) AND (date_dim.d_date <= cast('1970-01-01' as date) + interval 11222 days)))