{{
    config(
        materialized = "table",
        dist = "customer_unique_id",
        unique_key = "customer_unique_id"
    )
}}

SELECT
    customer_id::VARCHAR AS customer_id,
    customer_unique_id::VARCHAR AS customer_unique_id,
    customer_zip_code_prefix::VARCHAR AS customer_zip_code_prefix,
    customer_city::VARCHAR AS customer_city,
    customer_state::VARCHAR(2) AS customer_state,
    CURRENT_TIMESTAMP::TIMESTAMP AS processing_date
FROM {{ source('raw_data', 'tb_customer') }}
