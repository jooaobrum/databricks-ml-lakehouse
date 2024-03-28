WITH orders_items as (

  SELECT t1.order_id,
        COUNT(t1.product_id) as total_items,
        SUM(t1.item_price) as item_price,
        SUM(t1.freight_price) as freight_price,
        SUM(t1.item_price) + SUM(t1.freight_price) as total_price,
        SUM(t1.item_price) / (SUM(t1.item_price) + SUM(t1.freight_price)) as pct_item_price,
        SUM(t1.freight_price) / (SUM(t1.item_price) + SUM(t1.freight_price)) as pct_freight_price



  FROM silver.olist_order_items as t1

  GROUP BY t1.order_id

)

SELECT 
        '{date}' as fs_reference_timestamp,
        t1.*, 
       CASE WHEN t2.order_estimated_delivery_timestamp < t2.order_delivered_customer_timestamp THEN 1 ELSE 0 END as late_order

FROM orders_items as t1
LEFT JOIN silver.olist_orders as t2
ON t1.order_id = t2.order_id

WHERE DATE_FORMAT(order_purchase_timestamp, 'yyyy-MM-dd') = '{date}'
