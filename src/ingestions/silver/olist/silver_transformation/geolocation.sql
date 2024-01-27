SELECT  geolocation_zip_code_prefix as geolocation_zip_code,
        geolocation_lat as geolocation_latitude,
        geolocation_lng as geolocation_longitude,
        geolocation_city,
        geolocation_state,
        '{ingestor_file}' as table_ingestor_file,
        '{task_key}_silver_ingestion' as table_task_key, 
        current_timestamp() as table_ingestor_timestamp
           

FROM {view_tmp}

