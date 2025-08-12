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

WITH tb_orders AS (
    SELECT
        order_id                                AS id_order,
        customer_id                             AS id_customer,
        order_status                            AS fl_order_status,
        order_purchase_timestamp                AS dh_order_purchase,
        order_approved_at                       AS dh_order_approved_at,
        order_delivered_carrier_date            AS dh_order_delivered_carrier,
        order_delivered_customer_date           AS dh_order_delivered_customer,
        order_estimated_delivery_date           AS dh_order_estimated_delivery,
        ROW_NUMBER() OVER(
            PARTITION BY order_id
            ORDER BY order_id DESC
        )        AS index
    FROM {{ ref('stg_orders') }}
), tb_orders_filtered AS (
    SELECT *
    FROM tb_orders
    WHERE index = 1
), hash_generation AS (
    SELECT
        id_order,
        id_customer,
        fl_order_status,
        dh_order_purchase,
        dh_order_approved_at,
        dh_order_delivered_carrier,
        dh_order_delivered_customer,
        dh_order_estimated_delivery,
        MD5(
            COALESCE(fl_order_status::VARCHAR,'')              || '|' ||
            COALESCE(dh_order_purchase::VARCHAR,'')            || '|' ||
            COALESCE(dh_order_approved_at::VARCHAR,'')         || '|' ||
            COALESCE(dh_order_delivered_carrier::VARCHAR,'')   || '|' ||
            COALESCE(dh_order_delivered_customer::VARCHAR,'')  || '|' ||
            COALESCE(dh_order_estimated_delivery::VARCHAR,'') 
        ) AS hc_row_hash,
        CURRENT_TIMESTAMP::TIMESTAMP AS processing_date
    FROM tb_orders_filtered
), delta AS (
    {{ hash_macro('hash_generation', 'id_order') }}
), tb_final AS (
    SELECT
        id_order,
        id_customer,
        dh_order_purchase,
        dh_order_approved_at,
        dh_order_delivered_carrier,
        dh_order_delivered_customer,
        dh_order_estimated_delivery,
        fl_order_status,
        hc_row_hash,
        processing_date
    FROM delta
)
SELECT *
FROM tb_final