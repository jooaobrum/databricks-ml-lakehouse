SELECT  seller_id,
        seller_zip_code_prefix as seller_zip_code,
        seller_city,
        seller_state,
        '{ingestor_file}' as table_ingestor_file,
        '{task_key}_silver_ingestion' as table_task_key, 
        current_timestamp() as table_ingestor_timestamp
           

FROM {view_tmp}

