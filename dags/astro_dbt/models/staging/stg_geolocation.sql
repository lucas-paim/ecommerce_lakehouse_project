{{
    config(
        materialized = "table",
        dist = "geolocation_zip_code_prefix"
    )
}}

SELECT
    geolocation_zip_code_prefix::VARCHAR AS geolocation_zip_code_prefix,
    geolocation_lat::DOUBLE PRECISION AS geolocation_lat,
    geolocation_lng::DOUBLE PRECISION AS geolocation_lng,
    geolocation_city::VARCHAR AS geolocation_city,
    geolocation_state::VARCHAR(2) AS geolocation_state,
    CURRENT_TIMESTAMP::TIMESTAMP AS processing_date
FROM {{ source('raw_data', 'tb_geolocation')}}