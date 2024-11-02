SELECT 
       '{dt_ingestion}' as dt_ref,
       t2.seller_id,
       COUNT(DISTINCT t2.order_id) as seller_total_orders,
       AVG(t3.review_score) as seller_average_review_score
FROM olist_silver.olist_orders as t1
LEFT JOIN olist_silver.olist_order_items as t2
ON t1.order_id = t2.order_id
LEFT JOIN olist_silver.olist_order_reviews as t3
ON t1.order_id = t3.order_id

WHERE DATE(t1.order_delivered_customer_timestamp) <= '{dt_ingestion}'
AND DATE(t1.order_delivered_customer_timestamp) > DATE_SUB('{dt_ingestion}', 30*6)


GROUP BY t2.seller_id
