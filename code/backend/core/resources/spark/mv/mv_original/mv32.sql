SELEC
FROM
	customer_address SEMI JOIN  ON (coalesce(substr(customer_address.ca_zip, 1, 5), ) = coalesce(substr(, ))	Join customer
WHERE
	customer.c_preferred_cust_flag = 'Y'
	AND customer_address.ca_address_sk = customer.c_current_addr_sk