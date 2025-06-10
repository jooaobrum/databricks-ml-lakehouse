SELECT  customer_id,
        customer_unique_id,
        customer_zip_code_prefix as customer_zip_code,
        customer_city,
        customer_state


FROM bronze_{task_key}
