SELECT  product_id,
        product_category_name as product_category_name_portuguese,
        product_name_lenght,
        product_description_lenght,
        product_photos_qty,
        product_weight_g,
        product_weight_g/1000 as product_weight_kg,
        product_length_cm,
        product_height_cm,
        product_width_cm,
        product_length_cm*product_height_cm*product_width_cm as product_vol_cm3,
        '{ingestor_file}' as table_ingestor_file,
        '{task_key}_silver_ingestion' as table_task_key, 
        current_timestamp() as table_ingestor_timestamp
           

FROM {view_tmp}

