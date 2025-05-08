SELECT  geolocation_zip_code_prefix as geolocation_zip_code,
        geolocation_lat as geolocation_latitude,
        geolocation_lng as geolocation_longitude,
        geolocation_city,
        geolocation_state

FROM bronze_{task_key}
