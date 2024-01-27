SELECT  customer_id,
        customer_unique_id,
        customer_zip_code_prefix as customer_zip_code,
        customer_city,
        customer_state,
        '{ingestor_file}' as table_ingestor_file,
        '{task_key}_silver_ingestion' as table_task_key, 
        current_timestamp() as table_ingestor_timestamp
           

FROM {view_tmp}

