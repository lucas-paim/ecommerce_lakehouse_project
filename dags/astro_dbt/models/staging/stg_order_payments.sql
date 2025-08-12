{{
    config(
        materialized = "table",
        dist = "order_id",
        unique_key = "order_id"
    )
}}

SELECT
    order_id::VARCHAR AS order_id,
    payment_sequential::INTEGER AS payment_sequential,
    payment_type::VARCHAR AS payment_type,
    payment_installments::INTEGER AS payment_installments,
    payment_value::DECIMAL(10,2) AS payment_value,
    CURRENT_TIMESTAMP::TIMESTAMP AS processing_date
FROM {{ source('raw_data', 'tb_order_payments')}}