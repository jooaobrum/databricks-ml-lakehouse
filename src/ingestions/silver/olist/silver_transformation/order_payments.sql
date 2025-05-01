SELECT  order_id,
        payment_sequential,
        payment_type,
        payment_installments,
        payment_value,
        '{task_key}_silver_ingestion' as table_task_key,
        DATE_FORMAT(current_timestamp(), 'yyyy-MM-dd') as dt_ingestion


FROM {view_tmp}
