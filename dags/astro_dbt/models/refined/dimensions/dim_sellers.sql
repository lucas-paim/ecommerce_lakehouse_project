{{
    config(
        materialized = "table",
        unique_key = "id_seller"
    )
}}

WITH tb_seller AS (
    SELECT
        id_seller,
        abbr_seller_zip_code,
        abbr_seller_state,
        nm_seller_city
    FROM {{ ref('tru_sellers') }}
), tb_final AS (
    SELECT
        *,
        CURRENT_TIMESTAMP::TIMESTAMP AS processing_date
    FROM tb_seller
)
SELECT *
FROM tb_final