SELECT  order_id,
        order_item_id,
        product_id,
        seller_id,
        shipping_limit_date as shipping_limit_timestamp,
        price as item_price,
        freight_value as freight_price,
        '{task_key}_silver_ingestion' as table_task_key,
        DATE_FORMAT(current_timestamp(), 'yyyy-MM-dd') as dt_ingestion


FROM {view_tmp}
