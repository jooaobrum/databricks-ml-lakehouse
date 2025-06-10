SELECT  order_id,
        payment_sequential,
        payment_type,
        payment_installments,
        payment_value

FROM bronze_{task_key}
