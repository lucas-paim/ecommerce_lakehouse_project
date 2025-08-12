{{
    config(
        materialized = 'incremental',
        incremental_strategy = 'merge',
        unique_key = 'id_distinctive_customer',
        on_schema_change = 'sync_all_columns',
        post_hook = [
            "CREATE UNIQUE index IF NOT EXISTS {{ this.name }}_uk on {{ this }} (id_distinctive_customer)"
            ]
    )
}}

WITH tb_customer AS (
    SELECT
        customer_id                 AS id_customer,
        customer_unique_id          AS id_distinctive_customer,
        customer_zip_code_prefix    AS abbr_customer_zip_code,
        customer_city               AS nm_customer_city,
        customer_state              AS abbr_customer_state,
        ROW_NUMBER() OVER(
            PARTITION BY customer_unique_id
            ORDER BY customer_unique_id DESC
        )        AS index
    FROM {{ ref('stg_customer') }}
), tb_customer_filtered AS (
    SELECT *
    FROM tb_customer
    WHERE index = 1
), hash_generation AS (
    SELECT
        id_customer,
        id_distinctive_customer,
        abbr_customer_zip_code,
        nm_customer_city,
        abbr_customer_state,
        MD5(
            COALESCE(abbr_customer_zip_code::VARCHAR,'')     || '|' ||
            COALESCE(nm_customer_city::VARCHAR,'')           || '|' ||
            COALESCE(abbr_customer_state::VARCHAR,'') 
        ) AS hc_row_hash,
        CURRENT_TIMESTAMP::TIMESTAMP AS processing_date
    FROM tb_customer_filtered
), delta AS (
    {{ hash_macro('hash_generation', 'id_distinctive_customer') }}
), tb_final AS (
    SELECT
        id_distinctive_customer,
        id_customer,
        abbr_customer_zip_code,
        abbr_customer_state,
        nm_customer_city,
        hc_row_hash,
        processing_date
    FROM delta
)
SELECT *
FROM tb_final