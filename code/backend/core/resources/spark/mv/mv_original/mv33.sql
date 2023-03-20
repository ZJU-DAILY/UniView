SELEC
FROM
	customer_address SEMI JOIN  ON (coalesce(substr(customer_address.ca_zip, 1, 5), ) = coalesce(substr(, ))	Join date_dim
	Join store_sales
	Join store
	Join customer
WHERE
	store_sales.ss_store_sk = store.s_store_sk
	AND store_sales.ss_sold_date_sk = date_dim.d_date_sk
	AND substr(store.s_zip, 1, 2) = substr(customer_address.ca_zip, 1, 2)
	AND customer.c_preferred_cust_flag = 'Y'
	AND customer_address.ca_address_sk = customer.c_current_addr_sk
	AND date_dim.d_qoy = 2
	AND date_dim.d_year = 1998