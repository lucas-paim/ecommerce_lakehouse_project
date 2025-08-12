{{
    config(
        materialized = "table",
        unique_key = "id_product"
    )
}}

WITH tb_products AS (
    SELECT
        id_product,
        tp_product_category,          
        qt_product_name_lenght,            
        qt_product_description_lenght,  
        qt_product_photos_qty,         
        vl_product_weight_g,              
        vl_product_length_cm,               
        vl_product_height_cm,               
        vl_product_width_cm
    FROM {{ ref('tru_products') }}
), tb_final AS (
    SELECT
        *,
        CURRENT_TIMESTAMP::TIMESTAMP AS processing_date
    FROM tb_products
)
SELECT *
FROM tb_final