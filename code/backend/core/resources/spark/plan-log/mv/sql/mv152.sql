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
	item.i_current_price,
	date_dim.d_month_seq,
	item.i_wholesale_cost,
	item.i_item_id,
	inventory.inv_quantity_on_hand,
	item.i_manufact_id,
	item.i_item_desc,
	item.i_class,
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
	inventory.inv_date_sk = date_dim.d_date_sk
	AND (((inventory.inv_quantity_on_hand >= 100) AND (inventory.inv_quantity_on_hand <= 500)))
	AND item.i_item_sk = inventory.inv_item_sk
	AND (((item.i_manufact_id = 677) OR (item.i_manufact_id = 940) OR (item.i_manufact_id = 694) OR (item.i_manufact_id = 808)) OR ((item.i_manufact_id = 129) OR (item.i_manufact_id = 270) OR (item.i_manufact_id = 821) OR (item.i_manufact_id = 423)))