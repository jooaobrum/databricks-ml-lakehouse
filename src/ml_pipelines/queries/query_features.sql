with table_order as (

  SELECT t1.order_delivered_customer_timestamp as dt_ref,
        t1.order_id,
        AVG(t3.prod_average_review_score) as product_avg_review_score,
        AVG(t4.seller_total_orders) as seller_total_orders,
        AVG(t4.seller_average_review_score) as seller_avg_review_score
      
        

  FROM olist_silver.olist_orders as t1
  LEFT JOIN olist_silver.olist_order_items as t2
  ON t1.order_id = t2.order_id
  LEFT JOIN olist_feature_stores.olist_product_review_hist_features as t3
  ON t1.order_delivered_customer_timestamp = t3.dt_ref
  AND t2.product_id = t3.product_id
  LEFT JOIN olist_feature_stores.olist_seller_review_hist_features as t4
  ON t1.order_delivered_customer_timestamp = t4.dt_ref
  AND t2.seller_id = t4.seller_id

  WHERE t1.order_status = 'delivered'


  GROUP BY t1.order_delivered_customer_timestamp, t1.order_id
)

SELECT DISTINCT t1.dt_ref,
       t1.order_id,
       t1.product_avg_review_score,
       t1.seller_total_orders,
       t1.seller_avg_review_score,
       t2.days_to_estimation as order_day_to_estimation,
      t2.delivery_status as order_delivery_status,
      t2.days_after_carrier as order_days_after_carrier,
      t2.days_to_shipping_limit as order_days_to_shipping_limit,
      t2.shipping_status as order_shipping_status,
      t2.n_items as order_n_items,
      t2.items_price as order_items_price,
      t2.freight_price as order_freight_price,
      t2.total_order_price as order_total_price,
      t2.freight_proportion_price as order_freight_proportion_price,
      t2.payment_type as order_payment_type,
      t2.payment_installments as order_payment_installments,
      t3.beauty_health,
      t3.arts_entertainment,
      t3.sports_leisure,
      t3.baby_kids,
      t3.home_furniture,
      t3.electronics_appliances,
      t3.technology_gadgets,
      t3.construction_tools,
      t3.automotive_industry,
      t3.fashion_accessories,
      t3.avg_order_prod_description_length,
      t3.avg_order_photos_qty,
      t3.total_order_weight



FROM table_order as t1
INNER JOIN olist_feature_stores.olist_orders_delivered_features as t2
ON t1.dt_ref = t2.dt_ref
AND t1.order_id = t2.order_id
INNER JOIN olist_feature_stores.olist_products_features as t3
ON t1.dt_ref = t3.dt_ref
AND t1.order_id = t3.order_id


WHERE t1.dt_ref >= DATE('{dt_start}')
AND t1.dt_ref < DATE('{dt_stop}')