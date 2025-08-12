{{
    config(
        materialized = "table",
        unique_key = "id_distinctive_customer"
    )
}}

WITH tb_customer AS (
    SELECT
        id_distinctive_customer,
        id_customer,
        abbr_customer_zip_code,
        abbr_customer_state,
        nm_customer_city
    FROM {{ ref('tru_customer') }}
), tb_final AS (
    SELECT 
        *,
        CURRENT_TIMESTAMP::TIMESTAMP AS processing_date
    FROM tb_customer
)
SELECT *
FROM tb_final