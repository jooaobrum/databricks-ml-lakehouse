SELECT  seller_id,
        seller_zip_code_prefix as seller_zip_code,
        seller_city,
        seller_state,
        '{task_key}_silver_ingestion' as table_task_key,
        DATE_FORMAT(current_timestamp(), 'yyyy-MM-dd') as dt_ingestion


FROM {view_tmp}
