SELECT  product_category_name as product_category_name_portuguese,
        product_category_name_english,
        '{task_key}_silver_ingestion' as table_task_key,
        DATE_FORMAT(current_timestamp(), 'yyyy-MM-dd') as dt_ingestion


FROM {view_tmp}
