SELECT
	warehouse.w_city,
	item.i_brand,
	date_dim.d_month_seq,
	inventory.inv_warehouse_sk,
	item.i_brand_id,
	date_dim.d_date_sk,
	date_dim.d_qoy,
	inventory.inv_quantity_on_hand,
	warehouse.w_warehouse_sq_ft,
	date_dim.d_date,
	warehouse.w_county,
	item.i_manager_id,
	item.i_class_id,
	item.i_item_desc,
	item.i_class,
	warehouse.w_warehouse_name,
	item.i_category,
	warehouse.w_country,
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
	warehouse.w_warehouse_sk,
	date_dim.d_day_name,
	item.i_category_id,
	date_dim.d_dom,
	inventory.inv_item_sk,
	item.i_manufact_id,
	warehouse.w_state,
	date_dim.d_dow
FROM
	date_dim,
	inventory,
	warehouse,
	item
WHERE
	inventory.inv_date_sk = date_dim.d_date_sk
	AND inventory.inv_warehouse_sk = warehouse.w_warehouse_sk
	AND inventory.inv_item_sk = item.i_item_sk
	AND (date_dim.d_moy = 1 OR date_dim.d_moy = 2)
	AND date_dim.d_year = 2001