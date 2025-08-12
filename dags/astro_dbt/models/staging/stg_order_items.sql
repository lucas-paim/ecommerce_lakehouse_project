{{
    config(
        materialized = "table",
        dist = "order_id",
        unique_key = "order_id"
    )
}}

SELECT
    order_id::VARCHAR AS order_id,
    order_item_id::VARCHAR AS order_item_id,
    product_id::VARCHAR AS product_id,
    seller_id::VARCHAR AS seller_id,
    shipping_limit_date::TIMESTAMP AS shipping_limit_date,
    price:: DECIMAL(10,2) AS price,
    freight_value:: DECIMAL(10,2) AS freight_value,
    CURRENT_TIMESTAMP::TIMESTAMP AS processing_date
FROM {{ source('raw_data', 'tb_order_items')}}