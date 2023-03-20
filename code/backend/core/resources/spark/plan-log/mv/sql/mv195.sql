SELECT
	item.i_category_id,
	item.i_manufact,
	store_sales.ss_promo_sk,
	store_sales.ss_ext_discount_amt,
	store_sales.ss_store_sk,
	item.i_manager_id,
	store_sales.ss_item_sk,
	store_sales.ss_customer_sk,
	item.i_class_id,
	item.i_color,
	store_sales.ss_sold_date_sk,
	store_sales.ss_cdemo_sk,
	store_sales.ss_coupon_amt,
	item.i_size,
	store_sales.ss_addr_sk,
	item.i_units,
	item.i_item_sk,
	item.i_brand_id,
	item.i_product_name,
	store_sales.ss_ext_sales_price,
	item.i_brand,
	store_sales.ss_ticket_number,
	store_sales.ss_list_price,
	item.i_current_price,
	item.i_wholesale_cost,
	item.i_item_id,
	store_sales.ss_quantity,
	store_sales.ss_ext_tax,
	store_sales.ss_wholesale_cost,
	store_sales.ss_ext_wholesale_cost,
	item.i_manufact_id,
	store_sales.ss_ext_list_price,
	store_sales.ss_sales_price,
	item.i_class,
	store_sales.ss_sold_time_sk,
	item.i_item_desc,
	item.i_category,
	store_sales.ss_net_profit,
	store_sales.ss_hdemo_sk,
	store_sales.ss_net_paid
FROM
	store_sales,
	item
WHERE
	(((item.i_class = 'personal') OR (item.i_class = 'portable') OR (item.i_class = 'reference') OR (item.i_class = 'self-help') OR (item.i_class = 'accessories') OR (item.i_class = 'classical') OR (item.i_class = 'fragrances') OR (item.i_class = 'pants')))
	AND (((item.i_brand = 'scholaramalgamalg #14') OR (item.i_brand = 'scholaramalgamalg #7') OR (item.i_brand = 'exportiunivamalg #9') OR (item.i_brand = 'scholaramalgamalg #9') OR (item.i_brand = 'amalgimporto #1') OR (item.i_brand = 'edu packscholar #1') OR (item.i_brand = 'exportiimporto #1') OR (item.i_brand = 'importoamalg #1')))
	AND (((item.i_category = 'Books') OR (item.i_category = 'Children') OR (item.i_category = 'Electronics') OR (item.i_category = 'Women') OR (item.i_category = 'Music') OR (item.i_category = 'Men')))
	AND item.i_item_sk = store_sales.ss_item_sk