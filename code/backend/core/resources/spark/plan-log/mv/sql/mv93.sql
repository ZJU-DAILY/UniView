SELECT
	inventory.inv_item_sk,
	date_dim.d_dow,
	date_dim.d_quarter_name,
	date_dim.d_month_seq,
	date_dim.d_day_name,
	inventory.inv_warehouse_sk,
	date_dim.d_year,
	inventory.inv_quantity_on_hand,
	date_dim.d_qoy,
	date_dim.d_dom,
	date_dim.d_moy,
	inventory.inv_date_sk,
	date_dim.d_date,
	date_dim.d_week_seq,
	date_dim.d_date_sk
FROM
	date_dim,
	inventory
WHERE
	inventory.inv_date_sk = date_dim.d_date_sk
	AND (((date_dim.d_month_seq >= 1200) AND (date_dim.d_month_seq <= 1211)))