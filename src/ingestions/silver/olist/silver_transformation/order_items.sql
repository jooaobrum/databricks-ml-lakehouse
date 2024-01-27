SELECT  order_id,
        order_item_id,
        product_id,
        seller_id,
        shipping_limit_date as shipping_limit_timestamp,
        price as item_price,
        freight_value as freight_price,
        '{ingestor_file}' as table_ingestor_file,
        '{task_key}_silver_ingestion' as table_task_key, 
        current_timestamp() as table_ingestor_timestamp
           

FROM {view_tmp}

