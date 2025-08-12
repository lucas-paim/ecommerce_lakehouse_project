{{
    config(
        materialized = 'incremental',
        incremental_strategy = 'merge',
        unique_key = 'sk_order_item',
        on_schema_change = 'sync_all_columns',
        post_hook = [
            "CREATE UNIQUE index IF NOT EXISTS {{ this.name }}_uk on {{ this }} (sk_order_item)"
            ]
    )
}}

WITH tb_order_items AS (
    SELECT
        order_id                    AS id_order,
        order_item_id               AS id_order_item,
        product_id                  AS id_product,
        seller_id                   AS id_seller,
        shipping_limit_date         AS dh_ship_limit_date,
        price                       AS vl_price,
        freight_value               AS vl_freight_value,
        ROW_NUMBER() OVER(
            PARTITION BY order_id, order_item_id
            ORDER BY order_id, order_item_id DESC
        )        AS index
    FROM {{ ref('stg_order_items') }}
), tb_order_items_filtered AS (
    SELECT *
    FROM tb_order_items
    WHERE index = 1
), key_build AS (
    SELECT
        *,
        MD5(CONCAT(id_order, id_order_item)) AS sk_order_item
    FROM tb_order_items_filtered
), hash_generation AS (
    SELECT
        sk_order_item,
        id_order,
        id_order_item,
        id_product,
        id_seller,
        dh_ship_limit_date,
        vl_price,
        vl_freight_value,
        MD5(
            COALESCE(id_product::VARCHAR,'')                    || '|' ||
            COALESCE(id_seller::VARCHAR,'')                     || '|' ||
            COALESCE(dh_ship_limit_date::VARCHAR,'')            || '|' ||
            COALESCE(ROUND(vl_price, 2)::VARCHAR,'')            || '|' ||
            COALESCE(ROUND(vl_freight_value, 2)::VARCHAR,'')    
        ) AS hc_row_hash,
        CURRENT_TIMESTAMP::TIMESTAMP AS processing_date
    FROM key_build
), delta AS (
    {{ hash_macro('hash_generation', 'sk_order_item') }}
), tb_final AS (
    SELECT
        sk_order_item,
        id_order,
        id_order_item,
        id_product,
        id_seller,
        dh_ship_limit_date,
        vl_price,
        vl_freight_value,
        hc_row_hash,
        processing_date
    FROM delta       
)
SELECT *
FROM tb_final

