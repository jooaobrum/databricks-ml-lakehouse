SELECT t1.order_delivered_customer_timestamp,
       t1.order_id,
       CASE WHEN t2.review_score < 3 THEN 1 ELSE 0 END as bad_review
FROM olist_silver.olist_orders as t1
LEFT JOIN olist_silver.olist_order_reviews as t2
ON t1.order_id = t2.order_id

WHERE t2.review_score IS NOT NULL
AND t1.order_delivered_customer_timestamp >= DATE('{dt_start}')
AND t1.order_delivered_customer_timestamp < DATE('{dt_stop}')
