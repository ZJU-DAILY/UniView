SELECT
	inventory.inv_item_sk,
	warehouse.w_state,
	warehouse.w_warehouse_sq_ft,
	warehouse.w_warehouse_name,
	inventory.inv_date_sk,
	inventory.inv_warehouse_sk,
	warehouse.w_warehouse_sk,
	inventory.inv_quantity_on_hand,
	warehouse.w_county,
	warehouse.w_city,
	warehouse.w_country
FROM
	inventory,
	warehouse
WHERE
	inventory.inv_warehouse_sk = warehouse.w_warehouse_sk