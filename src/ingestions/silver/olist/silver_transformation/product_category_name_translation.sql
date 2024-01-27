SELECT  product_category_name as product_category_name_portuguese,
        product_category_name_english,
        '{ingestor_file}' as table_ingestor_file,
        '{task_key}_silver_ingestion' as table_task_key, 
        current_timestamp() as table_ingestor_timestamp
           

FROM {view_tmp}

