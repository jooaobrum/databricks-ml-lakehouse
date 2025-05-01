with orders_delivery as (
        SELECT  DISTINCT
                t1.order_id,
                DATEDIFF(t1.order_delivered_customer_timestamp, t1.order_estimated_delivery_timestamp) as days_to_estimation,
                CASE WHEN DATEDIFF(t1.order_delivered_customer_timestamp, t1.order_estimated_delivery_timestamp) > 0 THEN 'delivered_late' ELSE 'delivered_on_time' END as delivery_status,
                DATEDIFF(t1.order_delivered_customer_timestamp, t1.order_delivered_carrier_timestamp) as days_after_carrier,
                DATEDIFF(t1.order_delivered_carrier_timestamp, t2.shipping_limit_timestamp) as days_to_shipping_limit,
                CASE WHEN DATEDIFF(t1.order_delivered_carrier_timestamp, t2.shipping_limit_timestamp) > 0 THEN 'shipping_late' ELSE 'shipping_on_time' END as shipping_status


        FROM olist_silver.olist_orders as t1
        LEFT JOIN olist_silver.olist_order_items as t2
        ON t1.order_id = t2.order_id
),

orders_prices (
        SELECT order_id,
        MAX(order_item_id) as n_items,
        SUM(item_price) as items_price,
        FIRST(freight_price) as freight_price,
        SUM(item_price) + FIRST(freight_price) as total_order_price,
        FIRST(freight_price) / SUM(item_price) as freight_proportion_price

        FROM olist_silver.olist_order_items

        GROUP BY order_id
),

orders_payment (
        SELECT DISTINCT order_id,
                payment_type,
                payment_installments

        FROM olist_silver.olist_order_payments
)

SELECT t1.order_delivered_customer_timestamp as dt_ref,
       t1.order_id,
       t2.days_to_estimation,
       t2.delivery_status,
       t2.days_after_carrier,
       t2.days_to_shipping_limit,
       t2.shipping_status,
       t3.n_items,
       t3.items_price,
       t3.freight_price,
       t3.total_order_price,
       t3.freight_proportion_price,
       t4.payment_type,
       t4.payment_installments

FROM olist_silver.olist_orders as t1
INNER JOIN orders_delivery as t2
ON t1.order_id = t2.order_id
INNER JOIN orders_prices as t3
ON t1.order_id = t3.order_id
INNER JOIN orders_payment as t4
ON t1.order_id = t4.order_id

WHERE t1.order_status = 'delivered'
AND t1.order_delivered_customer_timestamp = '{dt_ingestion}'
