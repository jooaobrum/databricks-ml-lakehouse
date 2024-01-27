SELECT  review_id,
        order_id,
        review_score,
        review_comment_title,
        review_comment_message,
        review_creation_date as review_creation_timestamp,
        review_answer_timestamp,
        '{ingestor_file}' as table_ingestor_file,
        '{task_key}_silver_ingestion' as table_task_key, 
        current_timestamp() as table_ingestor_timestamp
           

FROM {view_tmp}

