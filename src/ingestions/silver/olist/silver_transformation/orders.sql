SELECT  order_id,
        customer_id,
        order_status,
        order_purchase_timestamp,
        order_approved_at as order_approved_at_timestamp,
        order_delivered_carrier_date as order_delivered_carrier_timestamp,
        order_delivered_customer_date as order_delivered_customer_timestamp,
        order_estimated_delivery_date as order_estimated_delivery_timestamp,
        '{task_key}_silver_ingestion' as table_task_key,
        DATE_FORMAT(current_timestamp(), 'yyyy-MM-dd') as dt_ingestion


FROM {view_tmp}
