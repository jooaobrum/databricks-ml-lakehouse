SELECT  seller_id,
        seller_zip_code_prefix as seller_zip_code,
        seller_city,
        seller_state

FROM bronze_{task_key}
