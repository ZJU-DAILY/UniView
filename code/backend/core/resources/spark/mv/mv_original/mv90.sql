SELECT
	warehouse.w_city,
	warehouse.w_warehouse_sk,
	inventory.inv_warehouse_sk,
	warehouse.w_warehouse_name,
	warehouse.w_country,
	inventory.inv_quantity_on_hand,
	inventory.inv_date_sk,
	inventory.inv_item_sk,
	warehouse.w_warehouse_sq_ft,
	warehouse.w_state,
	warehouse.w_county
FROM
	inventory,
	warehouse
WHERE
	inventory.inv_warehouse_sk = warehouse.w_warehouse_sk