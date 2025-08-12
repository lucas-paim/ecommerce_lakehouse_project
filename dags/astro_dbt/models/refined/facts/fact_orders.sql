{{
    config(
        materialized = "table",
        unique_key = "id_order"
    )
}}

WITH tb_orders AS (
    SELECT
        id_order,
        id_customer,
        dh_order_purchase,
        dh_order_approved_at,
        dh_order_delivered_carrier,
        dh_order_delivered_customer,
        dh_order_estimated_delivery,
        fl_order_status
    FROM {{ ref('tru_orders') }}
), tb_order_payments AS (
    SELECT
        id_order,
        nr_payment_sequential,
        nr_payment_installments,
        tp_payment_type,
        vl_payment_value
    FROM {{ ref('tru_order_payments') }}
), join_tables AS (
    SELECT
        o.id_order,
        o.id_customer,
        o.dh_order_purchase,
        o.dh_order_approved_at,
        o.dh_order_delivered_carrier,
        o.dh_order_delivered_customer,
        o.dh_order_estimated_delivery,
        o.fl_order_status,
        op.nr_payment_sequential,
        op.nr_payment_installments,
        op.tp_payment_type,
        vl_payment_value
    FROM tb_orders AS o
    LEFT JOIN tb_order_payments AS op
        ON o.id_order = op.id_order
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
        nr_payment_sequential,
        nr_payment_installments,
        tp_payment_type,
        vl_payment_value,
        CURRENT_TIMESTAMP::TIMESTAMP AS processing_date
    FROM join_tables
)
SELECT *
FROM tb_final

