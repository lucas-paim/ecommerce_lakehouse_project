{{
    config(
        materialized = "table",
        dist = "product_id",
        unique_key = "product_id"
    )
}}

SELECT
    product_id::VARCHAR AS product_id,
    product_category_name::VARCHAR AS product_category_name,
    product_name_lenght::INTEGER AS product_name_lenght,
    product_description_lenght::INTEGER AS product_description_lenght,
    product_photos_qty::INTEGER AS product_photos_qty,
    product_weight_g::DECIMAL(10,2) AS product_weight_g,
    product_length_cm::DECIMAL(10,2) AS product_length_cm,
    product_height_cm::DECIMAL(10,2) AS product_height_cm,
    product_width_cm::DECIMAL(10,2) AS product_width_cm,
    CURRENT_TIMESTAMP::TIMESTAMP AS processing_date
FROM {{ source('raw_data', 'tb_products')}}