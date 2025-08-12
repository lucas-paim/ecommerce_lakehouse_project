{{
    config(
        materialized = 'incremental',
        incremental_strategy = 'merge',
        unique_key = 'id_geolocation',
        on_schema_change = 'sync_all_columns',
        post_hook = [
            "CREATE UNIQUE index IF NOT EXISTS {{ this.name }}_uk on {{ this }} (id_geolocation)"
            ]
    )
}}

WITH tb_geolocation AS (
    SELECT
        geolocation_zip_code_prefix            AS abbr_zip_code,
        geolocation_lat                        AS vl_latitude,
        geolocation_lng                        AS vl_longitude,
        lower(unaccent(geolocation_city))      AS nm_city,
        upper(geolocation_state)               AS abbr_state,
        ROW_NUMBER() OVER(
            PARTITION BY geolocation_zip_code_prefix, geolocation_lat, geolocation_lng
            ORDER BY geolocation_zip_code_prefix DESC
        )        AS index
    FROM {{ ref('stg_geolocation') }}
), tb_geolocation_filtered AS (
    SELECT *
    FROM tb_geolocation
    WHERE index = 1
), key_build AS (
    SELECT
        *,
        MD5(CONCAT(abbr_zip_code, vl_latitude, vl_longitude)) AS id_geolocation
    FROM tb_geolocation_filtered
), hash_generation AS (
    SELECT
        id_geolocation,
        abbr_zip_code,
        vl_latitude,
        vl_longitude,
        nm_city,
        abbr_state,
        MD5(
            COALESCE(abbr_zip_code::VARCHAR,'')                      || '|' ||
            COALESCE(ROUND(vl_latitude::NUMERIC, 2)::VARCHAR,'')     || '|' ||
            COALESCE(ROUND(vl_longitude::NUMERIC, 2)::VARCHAR,'')    || '|' ||
            COALESCE(nm_city::VARCHAR,'')                            || '|' ||
            COALESCE(abbr_state::VARCHAR,'')
        ) AS hc_row_hash,
        CURRENT_TIMESTAMP::TIMESTAMP AS processing_date
    FROM key_build
), delta AS (
    {{ hash_macro('hash_generation', 'id_geolocation') }}
), tb_final AS (
    SELECT
        id_geolocation,
        abbr_zip_code,
        abbr_state,
        vl_latitude,
        vl_longitude,
        nm_city,
        hc_row_hash,
        processing_date
    FROM delta        
)
SELECT *
FROM tb_final