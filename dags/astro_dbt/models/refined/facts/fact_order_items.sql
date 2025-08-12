{{
    config(
        materialized = "table",
        unique_key = "sk_order_item"
    )
}}

WITH tb_order_items AS (
    SELECT
        sk_order_item,
        id_order,
        id_order_item,
        id_product,
        id_seller,
        dh_ship_limit_date,
        vl_price,
        vl_freight_value
    FROM {{ ref('tru_order_items') }}
), tb_final AS (
    SELECT
        *,
        CURRENT_TIMESTAMP::TIMESTAMP AS processing_date
    FROM tb_order_items
)
SELECT *
FROM tb_final