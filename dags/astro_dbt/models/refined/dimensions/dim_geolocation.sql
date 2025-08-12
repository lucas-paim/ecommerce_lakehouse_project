{{
    config(
        materialized = "table",
        unique_key = "id_geolocation"
    )
}}

WITH tb_geolocation AS (
    SELECT
        id_geolocation,
        abbr_zip_code,
        abbr_state,
        vl_latitude,
        vl_longitude,
        nm_city
    FROM {{ ref('tru_geolocation') }}
), tb_final AS (
    SELECT
        *,
        CURRENT_TIMESTAMP::TIMESTAMP AS processing_date
    FROM tb_geolocation
)
SELECT *
FROM tb_final