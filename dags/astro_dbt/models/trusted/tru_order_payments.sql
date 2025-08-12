{{
    config(
        materialized = 'incremental',
        incremental_strategy = 'merge',
        unique_key = 'id_order',
        on_schema_change = 'sync_all_columns',
        post_hook = [
            "CREATE UNIQUE index IF NOT EXISTS {{ this.name }}_uk on {{ this }} (id_order)"
            ]
    )
}}

WITH tb_order_payments AS (
    SELECT
        order_id                    AS id_order,
        payment_sequential          AS nr_payment_sequential,
        payment_type                AS tp_payment_type,
        payment_installments        AS nr_payment_installments,
        payment_value               AS vl_payment_value,
        ROW_NUMBER() OVER(
            PARTITION BY order_id
            ORDER BY order_id DESC
        )        AS index
    FROM {{ ref('stg_order_payments') }}
), tb_order_payments_filtered AS (
    SELECT *
    FROM tb_order_payments
    WHERE index = 1
), hash_generation AS (
    SELECT
        id_order,
        nr_payment_sequential,
        tp_payment_type,
        nr_payment_installments,
        vl_payment_value,
        MD5(
            COALESCE(nr_payment_sequential::VARCHAR,'')     || '|' ||
            COALESCE(tp_payment_type::VARCHAR,'')           || '|' ||
            COALESCE(nr_payment_installments::VARCHAR,'')   || '|' ||
            COALESCE(ROUND(vl_payment_value, 2)::VARCHAR,'')  
        ) AS hc_row_hash,
        CURRENT_TIMESTAMP::TIMESTAMP AS processing_date
    FROM tb_order_payments_filtered
), delta AS (
    {{ hash_macro('hash_generation', 'id_order') }}
), tb_final AS (
    SELECT
        id_order,
        nr_payment_sequential,
        nr_payment_installments,
        tp_payment_type,
        vl_payment_value,
        hc_row_hash,
        processing_date
    FROM delta
)
SELECT *
FROM tb_final