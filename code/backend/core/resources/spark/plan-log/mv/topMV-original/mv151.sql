SELECT
	inventory.inv_item_sk,
	item.i_product_name,
	item.i_manufact,
	item.i_brand,
	item.i_category_id,
	item.i_current_price,
	item.i_wholesale_cost,
	item.i_item_id,
	item.i_manager_id,
	inventory.inv_warehouse_sk,
	inventory.inv_quantity_on_hand,
	item.i_class_id,
	item.i_manufact_id,
	item.i_color,
	item.i_size,
	item.i_item_desc,
	item.i_class,
	inventory.inv_date_sk,
	item.i_units,
	item.i_category,
	item.i_item_sk,
	item.i_brand_id
FROM
	item,
	inventory
WHERE
	(((item.i_manufact_id = 677) OR (item.i_manufact_id = 940) OR (item.i_manufact_id = 694) OR (item.i_manufact_id = 808)) OR ((item.i_manufact_id = 129) OR (item.i_manufact_id = 270) OR (item.i_manufact_id = 821) OR (item.i_manufact_id = 423)))
	AND item.i_item_sk = inventory.inv_item_sk
	AND (((inventory.inv_quantity_on_hand >= 100) AND (inventory.inv_quantity_on_hand <= 500)))