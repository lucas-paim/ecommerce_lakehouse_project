{{
    config(
        materialized = "table",
        dist = "seller_id",
        unique_key = "seller_id"
    )
}}

SELECT
    seller_id:: VARCHAR AS seller_id,
    seller_zip_code_prefix::VARCHAR AS seller_zip_code_prefix,
    seller_city::VARCHAR AS seller_city,
    seller_state::VARCHAR(2) AS seller_state,
    CURRENT_TIMESTAMP::TIMESTAMP AS processing_date
FROM {{ source('raw_data', 'tb_sellers')}}