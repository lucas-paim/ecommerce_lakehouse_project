{{
    config(
        materialized = 'incremental',
        incremental_strategy = 'merge',
        unique_key = 'id_seller',
        on_schema_change = 'sync_all_columns',
        post_hook = [
            "CREATE UNIQUE index IF NOT EXISTS {{ this.name }}_uk on {{ this }} (id_seller)"
            ]
    )
}}

WITH tb_sellers AS (
    SELECT
    seller_id                   AS id_seller,
    seller_zip_code_prefix      AS abbr_seller_zip_code,
    seller_city                 AS nm_seller_city,
    seller_state                AS abbr_seller_state,
    ROW_NUMBER() OVER(
        PARTITION BY seller_id
        ORDER BY seller_id DESC
    )        AS index
    FROM {{ ref('stg_sellers') }}
), tb_sellers_filtered AS (
    SELECT *
    FROM tb_sellers
    WHERE index = 1
), hash_generation AS (
    SELECT
        id_seller,
        abbr_seller_zip_code,
        nm_seller_city,
        abbr_seller_state,
        MD5(
            COALESCE(abbr_seller_zip_code::VARCHAR,'') || '|' ||
            COALESCE(abbr_seller_state::VARCHAR,'')    || '|' ||
            COALESCE(nm_seller_city::VARCHAR,'')
        ) AS hc_row_hash,
        CURRENT_TIMESTAMP::TIMESTAMP AS processing_date
    FROM tb_sellers_filtered
), delta AS (
    {{ hash_macro('hash_generation', 'id_seller') }}
), tb_final AS(
    SELECT
        id_seller,
        abbr_seller_zip_code,
        abbr_seller_state,
        nm_seller_city,
        hc_row_hash,
        processing_date
    FROM delta
)
SELECT *
FROM tb_final