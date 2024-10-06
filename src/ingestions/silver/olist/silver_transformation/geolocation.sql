SELECT  geolocation_zip_code_prefix as geolocation_zip_code,
        geolocation_lat as geolocation_latitude,
        geolocation_lng as geolocation_longitude,
        geolocation_city,
        geolocation_state,
        '{task_key}_silver_ingestion' as table_task_key, 
        DATE_FORMAT(current_timestamp(), 'yyyy-MM-dd') as dt_ingestion           

FROM {view_tmp}

