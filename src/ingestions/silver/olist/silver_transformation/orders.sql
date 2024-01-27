SELECT  order_id,
        customer_id,
        order_status,
        order_purchase_timestamp,
        order_approved_at as order_approved_at_timestamp,
        order_delivered_carrier_date as order_delivered_carrier_timestamp,
        order_delivered_customer_date as order_delivered_customer_timestamp,
        order_estimated_delivery_date as order_estimated_delivery_timestamp,
        '{ingestor_file}' as table_ingestor_file,
        '{task_key}_silver_ingestion' as table_task_key, 
        current_timestamp() as table_ingestor_timestamp
           

FROM {view_tmp}

