SELECT  order_id,
        payment_sequential,
        payment_type,
        payment_installments,
        payment_value,
        '{ingestor_file}' as table_ingestor_file,
        '{task_key}_silver_ingestion' as table_task_key, 
        current_timestamp() as table_ingestor_timestamp
           

FROM {view_tmp}

