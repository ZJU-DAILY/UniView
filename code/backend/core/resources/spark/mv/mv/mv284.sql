CREATE MATERIALIZED VIEW IF NOT EXISTS mv284
PARTITIONED BY (cs_sold_date_sk)
AS
SELECT
	customer_demographics.cd_purchase_estimate,
	catalog_sales.cs_ship_customer_sk,
	promotion.p_channel_tv,
	inventory.inv_warehouse_sk,
	household_demographics.hd_vehicle_count,
	item.i_brand_id,
	catalog_sales.cs_catalog_page_sk,
	date_dim.d_qoy,
	customer_demographics.cd_dep_count,
	catalog_sales.cs_call_center_sk,
	warehouse.w_county,
	item.i_manager_id,
	catalog_sales.cs_bill_cdemo_sk,
	item.i_class,
	promotion.p_channel_event,
	catalog_sales.cs_bill_addr_sk,
	customer_demographics.cd_demo_sk,
	item.i_item_id,
	date_dim.d_moy,
	item.i_current_price,
	date_dim.d_week_seq,
	household_demographics.hd_dep_count,
	catalog_sales.cs_item_sk,
	catalog_sales.cs_bill_customer_sk,
	item.i_manufact,
	item.i_units,
	customer_demographics.cd_gender,
	household_demographics.hd_demo_sk,
	date_dim.d_quarter_name,
	item.i_product_name,
	customer_demographics.cd_dep_employed_count,
	catalog_sales.cs_sold_time_sk,
	household_demographics.hd_income_band_sk,
	item.i_color,
	catalog_sales.cs_sales_price,
	catalog_sales.cs_quantity,
	catalog_sales.cs_coupon_amt,
	date_dim.d_dom,
	inventory.inv_item_sk,
	item.i_manufact_id,
	catalog_sales.cs_net_paid_inc_tax,
	catalog_sales.cs_ext_discount_amt,
	promotion.p_channel_dmail,
	item.i_brand,
	warehouse.w_city,
	date_dim.d_month_seq,
	customer_demographics.cd_dep_college_count,
	catalog_sales.cs_ext_ship_cost,
	date_dim.d_date_sk,
	inventory.inv_quantity_on_hand,
	household_demographics.hd_buy_potential,
	promotion.p_channel_email,
	warehouse.w_warehouse_sq_ft,
	date_dim.d_date,
	item.i_class_id,
	item.i_item_desc,
	catalog_sales.cs_order_number,
	catalog_sales.cs_wholesale_cost,
	catalog_sales.cs_ext_sales_price,
	customer_demographics.cd_education_status,
	warehouse.w_warehouse_name,
	item.i_category,
	warehouse.w_country,
	catalog_sales.cs_net_paid,
	item.i_size,
	catalog_sales.cs_ext_wholesale_cost,
	catalog_sales.cs_ship_date_sk,
	date_dim.d_year,
	catalog_sales.cs_promo_sk,
	catalog_sales.cs_ship_mode_sk,
	catalog_sales.cs_ext_list_price,
	promotion.p_promo_sk,
	catalog_sales.cs_bill_hdemo_sk,
	inventory.inv_date_sk,
	customer_demographics.cd_marital_status,
	item.i_item_sk,
	catalog_sales.cs_warehouse_sk,
	item.i_wholesale_cost,
	customer_demographics.cd_credit_rating,
	warehouse.w_warehouse_sk,
	catalog_sales.cs_list_price,
	catalog_sales.cs_net_profit,
	catalog_sales.cs_ship_addr_sk,
	date_dim.d_day_name,
	item.i_category_id,
	warehouse.w_state,
	date_dim.d_dow,
	catalog_sales.cs_sold_date_sk
FROM
	catalog_sales LEFT OUTER JOIN promotion ON (catalog_sales.cs_promo_sk = promotion.p_promo_sk)	Join inventory
	Join item
	Join date_dim
	Join warehouse
	Join customer_demographics
	Join household_demographics
WHERE
	inventory.inv_date_sk = date_dim.d_date_sk
	AND inventory.inv_warehouse_sk = warehouse.w_warehouse_sk
	AND catalog_sales.cs_item_sk = inventory.inv_item_sk
	AND catalog_sales.cs_sold_date_sk = date_dim.d_date_sk
	AND catalog_sales.cs_ship_date_sk = date_dim.d_date_sk
	AND date_dim.d_year = 1999
	AND catalog_sales.cs_bill_cdemo_sk = customer_demographics.cd_demo_sk
	AND catalog_sales.cs_item_sk = item.i_item_sk
	AND catalog_sales.cs_bill_hdemo_sk = household_demographics.hd_demo_sk
	AND customer_demographics.cd_marital_status = 'D'
	AND household_demographics.hd_buy_potential = '>10000'
DISTRIBUTE BY cs_sold_date_sk;