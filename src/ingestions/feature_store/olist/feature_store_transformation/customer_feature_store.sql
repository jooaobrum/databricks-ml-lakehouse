WITH customer_order_timeline AS (
  SELECT
    t1.*,
    t2.customer_unique_id,
    t1.order_purchase_timestamp,
    ROW_NUMBER() OVER (PARTITION BY t2.customer_unique_id ORDER BY t1.order_purchase_timestamp) AS row_num_asc,
    ROW_NUMBER() OVER (PARTITION BY t2.customer_unique_id ORDER BY t1.order_purchase_timestamp DESC) AS row_num_desc
  FROM
    silver.olist_orders AS t1
    LEFT JOIN silver.olist_customers AS t2 ON t1.customer_id = t2.customer_id
    LEFT JOIN silver.olist_order_items AS t3 ON t1.order_id = t3.order_id
  WHERE
    t1.order_purchase_timestamp <= '{date}'
),

customers_orders_days_since (
SELECT
  customer_unique_id,
  MIN(CASE WHEN row_num_asc = 1 THEN datediff('{date}', order_purchase_timestamp) END) AS days_since_first_order,
  MIN(CASE WHEN row_num_desc = 1 THEN datediff('{date}', order_purchase_timestamp) END) AS days_since_last_order
FROM
  customer_order_timeline
GROUP BY
  customer_unique_id

),

lagged_timestamps AS (
  SELECT
    customer_unique_id,
    order_purchase_timestamp,
    order_id,
    LAG(order_purchase_timestamp) OVER (PARTITION BY customer_unique_id ORDER BY order_purchase_timestamp) AS lagged_timestamp
  FROM customer_order_timeline
),

delivery_times AS (
  SELECT
    customer_unique_id,
    AVG(DATEDIFF(order_delivered_customer_timestamp, order_purchase_timestamp)) AS avg_days_to_deliver
  FROM customer_order_timeline
  WHERE order_delivered_customer_timestamp IS NOT NULL

  GROUP BY customer_unique_id
),


late_deliveries AS (
  SELECT customer_unique_id,
  SUM(CASE WHEN DATEDIFF(order_delivered_customer_timestamp, order_estimated_delivery_timestamp) > 0 THEN 1 ELSE 0 END) as total_orders_late_delivery,
  SUM(CASE WHEN DATEDIFF(order_delivered_customer_timestamp, order_estimated_delivery_timestamp) < 0 THEN 1 ELSE 0 END) as total_orders_early_delivery

  FROM customer_order_timeline
  WHERE order_delivered_customer_timestamp IS NOT NULL
  GROUP BY customer_unique_id

),

customer_feature_store as (
  SELECT
    '{date}' AS fs_reference_timestamp,
    t1.customer_unique_id,
    t1.days_since_first_order,
    t1.days_since_last_order,
    t2.customer_city,
    t2.customer_state,
    t3.avg_days_to_deliver,
    t4.total_orders_late_delivery,
    t4.total_orders_early_delivery
    
  FROM
    customers_orders_days_since AS t1 
    LEFT JOIN silver.olist_customers as t2 ON t1.customer_unique_id = t2.customer_unique_id
    LEFT JOIN delivery_times as t3 ON t1.customer_unique_id = t3.customer_unique_id
    LEFT JOIN late_deliveries as t4 ON t1.customer_unique_id = t4.customer_unique_id

)

SELECT * FROM customer_feature_store;