SELECT
	catalog_sales.cs_ext_ship_cost,
	catalog_sales.cs_wholesale_cost,
	catalog_sales.cs_quantity,
	catalog_sales.cs_net_profit,
	catalog_sales.cs_net_paid,
	catalog_sales.cs_sold_time_sk,
	catalog_sales.cs_item_sk,
	catalog_sales.cs_list_price,
	catalog_returns.cr_returned_date_sk,
	catalog_sales.cs_order_number,
	catalog_returns.cr_call_center_sk,
	catalog_returns.cr_order_number,
	catalog_returns.cr_return_amt_inc_tax,
	catalog_sales.cs_ext_wholesale_cost,
	catalog_sales.cs_ext_sales_price,
	catalog_sales.cs_catalog_page_sk,
	catalog_sales.cs_call_center_sk,
	catalog_sales.cs_sales_price,
	catalog_sales.cs_ship_addr_sk,
	catalog_returns.cr_returning_addr_sk,
	catalog_returns.cr_store_credit,
	catalog_sales.cs_net_paid_inc_tax,
	catalog_returns.cr_returning_customer_sk,
	catalog_returns.cr_return_amount,
	catalog_sales.cs_bill_cdemo_sk,
	catalog_sales.cs_warehouse_sk,
	catalog_sales.cs_ship_customer_sk,
	catalog_returns.cr_item_sk,
	catalog_returns.cr_refunded_cash,
	catalog_sales.cs_ship_mode_sk,
	catalog_returns.cr_net_loss,
	catalog_returns.cr_return_quantity,
	catalog_sales.cs_sold_date_sk,
	catalog_sales.cs_ext_list_price,
	catalog_sales.cs_coupon_amt,
	catalog_sales.cs_ship_date_sk,
	catalog_sales.cs_bill_customer_sk,
	catalog_sales.cs_ext_discount_amt,
	catalog_sales.cs_promo_sk,
	catalog_returns.cr_catalog_page_sk,
	catalog_sales.cs_bill_addr_sk,
	catalog_sales.cs_bill_hdemo_sk,
	catalog_returns.cr_reversed_charge
FROM
	catalog_returns,
	catalog_sales
WHERE
	catalog_sales.cs_item_sk = catalog_returns.cr_item_sk
	AND catalog_sales.cs_order_number = catalog_returns.cr_order_number