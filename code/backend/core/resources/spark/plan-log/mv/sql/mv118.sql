SELECT
	catalog_sales.cs_ext_ship_cost,
	catalog_sales.cs_wholesale_cost,
	catalog_sales.cs_quantity,
	customer_demographics.cd_education_status,
	catalog_sales.cs_net_profit,
	catalog_sales.cs_net_paid,
	catalog_sales.cs_sold_time_sk,
	catalog_sales.cs_item_sk,
	catalog_sales.cs_list_price,
	customer_demographics.cd_dep_employed_count,
	catalog_sales.cs_order_number,
	customer_demographics.cd_dep_college_count,
	customer_demographics.cd_demo_sk,
	catalog_sales.cs_ext_wholesale_cost,
	catalog_sales.cs_ext_sales_price,
	catalog_sales.cs_catalog_page_sk,
	catalog_sales.cs_call_center_sk,
	customer_demographics.cd_credit_rating,
	catalog_sales.cs_sales_price,
	catalog_sales.cs_ship_addr_sk,
	catalog_sales.cs_net_paid_inc_tax,
	catalog_sales.cs_bill_cdemo_sk,
	catalog_sales.cs_warehouse_sk,
	catalog_sales.cs_ship_customer_sk,
	customer_demographics.cd_purchase_estimate,
	customer_demographics.cd_marital_status,
	catalog_sales.cs_ship_mode_sk,
	customer_demographics.cd_dep_count,
	catalog_sales.cs_sold_date_sk,
	catalog_sales.cs_ext_list_price,
	catalog_sales.cs_coupon_amt,
	catalog_sales.cs_ship_date_sk,
	catalog_sales.cs_bill_customer_sk,
	catalog_sales.cs_ext_discount_amt,
	catalog_sales.cs_promo_sk,
	customer_demographics.cd_gender,
	catalog_sales.cs_bill_addr_sk,
	catalog_sales.cs_bill_hdemo_sk
FROM
	catalog_sales,
	customer_demographics
WHERE
	customer_demographics.cd_gender = 'M'
	AND customer_demographics.cd_education_status = 'College'
	AND catalog_sales.cs_bill_cdemo_sk = customer_demographics.cd_demo_sk
	AND customer_demographics.cd_marital_status = 'S'