SELECT  order_id,
        order_item_id,
        product_id,
        seller_id,
        shipping_limit_date as shipping_limit_timestamp,
        price as item_price,
        freight_value as freight_price

FROM bronze_{task_key}
