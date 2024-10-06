SELECT  customer_id,
        customer_unique_id,
        customer_zip_code_prefix as customer_zip_code,
        customer_city,
        customer_state,
        '{task_key}_silver_ingestion' as table_task_key, 
        DATE_FORMAT(current_timestamp(), 'yyyy-MM-dd') as dt_ingestion
           

FROM {view_tmp}

