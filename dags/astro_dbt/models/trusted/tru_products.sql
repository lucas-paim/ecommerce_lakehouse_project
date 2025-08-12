{{
    config(
        materialized = 'incremental',
        incremental_strategy = 'merge',
        unique_key = 'id_product',
        on_schema_change = 'sync_all_columns',
        post_hook = [
            "CREATE UNIQUE index IF NOT EXISTS {{ this.name }}_uk on {{ this }} (id_product)"
            ]
    )
}}

WITH tb_products AS (
    SELECT
        product_id                      AS id_product,
        product_category_name           AS tp_product_category,
        product_name_lenght             AS qt_product_name_lenght,
        product_description_lenght      AS qt_product_description_lenght,
        product_photos_qty              AS qt_product_photos_qty,
        product_weight_g                AS vl_product_weight_g,
        product_length_cm               AS vl_product_length_cm,
        product_height_cm               AS vl_product_height_cm,
        product_width_cm                AS vl_product_width_cm,
        ROW_NUMBER() OVER(
            PARTITION BY product_id
            ORDER BY product_id DESC
        )        AS index
    FROM {{ ref('stg_products') }}
), tb_products_filtered AS (
    SELECT *
    FROM tb_products
    WHERE index = 1
), hash_generation AS (
    SELECT
        id_product,
        tp_product_category,
        qt_product_name_lenght,
        qt_product_description_lenght,
        qt_product_photos_qty,
        vl_product_weight_g,
        vl_product_length_cm,
        vl_product_height_cm,
        vl_product_width_cm,
        MD5(
            COALESCE(tp_product_category::VARCHAR,'')               || '|' ||
            COALESCE(qt_product_name_lenght::VARCHAR,'')            || '|' ||
            COALESCE(qt_product_description_lenght::VARCHAR,'')     || '|' ||
            COALESCE(qt_product_photos_qty::VARCHAR,'')             || '|' ||
            COALESCE(ROUND(vl_product_weight_g, 2)::VARCHAR,'')     || '|' ||
            COALESCE(ROUND(vl_product_length_cm, 2)::VARCHAR,'')    || '|' ||
            COALESCE(ROUND(vl_product_height_cm, 2)::VARCHAR,'')    || '|' ||
            COALESCE(ROUND(vl_product_width_cm, 2)::VARCHAR,'')
        ) AS hc_row_hash,
        CURRENT_TIMESTAMP::TIMESTAMP AS processing_date
    FROM tb_products_filtered
), delta AS (
    {{ hash_macro('hash_generation', 'id_product') }}
), tb_final AS (
    SELECT
        id_product,
        tp_product_category,          
        qt_product_name_lenght,            
        qt_product_description_lenght,  
        qt_product_photos_qty,         
        vl_product_weight_g,              
        vl_product_length_cm,               
        vl_product_height_cm,               
        vl_product_width_cm,
        hc_row_hash,
        processing_date
    FROM delta            
)
SELECT *
FROM tb_final