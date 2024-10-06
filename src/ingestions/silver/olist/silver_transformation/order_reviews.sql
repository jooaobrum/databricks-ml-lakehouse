SELECT  review_id,
        order_id,
        review_score,
        review_comment_title,
        review_comment_message,
        review_creation_date as review_creation_timestamp,
        review_answer_timestamp,
        '{task_key}_silver_ingestion' as table_task_key, 
        DATE_FORMAT(current_timestamp(), 'yyyy-MM-dd') as dt_ingestion           
           

FROM {view_tmp}

