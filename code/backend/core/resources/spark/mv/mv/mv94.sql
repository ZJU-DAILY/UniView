CREATE MATERIALIZED VIEW IF NOT EXISTS mv94
AS
SELECT
	item.i_brand,
	date_dim.d_month_seq,
	inventory.inv_warehouse_sk,
	item.i_brand_id,
	date_dim.d_date_sk,
	date_dim.d_qoy,
	inventory.inv_quantity_on_hand,
	date_dim.d_date,
	item.i_manager_id,
	item.i_class_id,
	item.i_item_desc,
	item.i_class,
	item.i_category,
	item.i_size,
	item.i_item_id,
	date_dim.d_moy,
	item.i_current_price,
	date_dim.d_week_seq,
	date_dim.d_year,
	item.i_manufact,
	item.i_units,
	date_dim.d_quarter_name,
	inventory.inv_date_sk,
	item.i_item_sk,
	item.i_product_name,
	item.i_wholesale_cost,
	item.i_color,
	date_dim.d_day_name,
	item.i_category_id,
	date_dim.d_dom,
	inventory.inv_item_sk,
	item.i_manufact_id,
	date_dim.d_dow
FROM
	date_dim,
	inventory,
	item
WHERE
	inventory.inv_date_sk = date_dim.d_date_sk
	AND inventory.inv_item_sk = item.i_item_sk
	AND (((date_dim.d_month_seq >= 1200) AND (date_dim.d_month_seq <= 1211)));