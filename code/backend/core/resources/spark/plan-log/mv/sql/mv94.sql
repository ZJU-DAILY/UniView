SELECT
	inventory.inv_item_sk,
	date_dim.d_dow,
	item.i_manufact,
	date_dim.d_quarter_name,
	date_dim.d_day_name,
	item.i_manager_id,
	inventory.inv_warehouse_sk,
	date_dim.d_qoy,
	item.i_class_id,
	date_dim.d_dom,
	item.i_color,
	item.i_size,
	inventory.inv_date_sk,
	item.i_units,
	date_dim.d_week_seq,
	date_dim.d_year,
	item.i_item_sk,
	item.i_brand_id,
	item.i_product_name,
	item.i_brand,
	date_dim.d_month_seq,
	item.i_current_price,
	item.i_wholesale_cost,
	item.i_item_id,
	inventory.inv_quantity_on_hand,
	item.i_manufact_id,
	item.i_class,
	item.i_item_desc,
	date_dim.d_moy,
	date_dim.d_date,
	item.i_category,
	date_dim.d_date_sk,
	item.i_category_id
FROM
	item,
	date_dim,
	inventory
WHERE
	(((date_dim.d_month_seq >= 1200) AND (date_dim.d_month_seq <= 1211)))
	AND inventory.inv_date_sk = date_dim.d_date_sk
	AND inventory.inv_item_sk = item.i_item_sk