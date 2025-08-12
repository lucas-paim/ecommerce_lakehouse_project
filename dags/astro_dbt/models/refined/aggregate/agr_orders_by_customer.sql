{{
    config(
        materialized = "table"
    )
}}

WITH tb_customer AS (
    SELECT
        id_distinctive_customer,
        id_customer
    FROM {{ ref('tru_customer') }}
), tb_orders AS (
    SELECT
        id_order,
        id_customer,
        dh_order_purchase
    FROM {{ ref('tru_orders') }}
), tb_order_payments AS (
    SELECT
        id_order,
        vl_payment_value
    FROM {{ ref('tru_order_payments') }}
), tb_order_reviews AS (
    SELECT
        id_order,
        vl_review_score
    FROM {{ ref('tru_order_reviews') }}
), join_tables AS (
    SELECT
        cm.id_customer,
        cm.id_distinctive_customer,
        o.id_order,
        o.dh_order_purchase,
        op.vl_payment_value,
        orv.vl_review_score
    FROM tb_customer AS cm
    LEFT JOIN tb_orders AS o
        ON
        cm.id_customer = o.id_customer
    LEFT JOIN tb_order_payments AS op
        ON
        o.id_order = op.id_order
    LEFT JOIN tb_order_reviews AS orv
        ON
        o.id_order = orv.id_order
), data_transform AS (
    SELECT
        id_distinctive_customer,
        CAST(MIN(dh_order_purchase) AS DATE) AS dt_first_purchase,
        CAST(MAX(dh_order_purchase) AS DATE) AS dt_last_purchase,
        CURRENT_DATE - MAX(dh_order_purchase) AS nr_days_since_last_purchase,
        COUNT(DISTINCT id_order) AS qt_total_orders,
        SUM(vl_payment_value) / COUNT(DISTINCT id_order) AS vl_avg_ticket,
        SUM(vl_payment_value) AS vl_total_spent,
        AVG(vl_review_score) AS vl_avg_review_score
    FROM join_tables
    GROUP BY id_distinctive_customer
), key_build AS (
    SELECT
        *,
        id_distinctive_customer AS "key"
    FROM data_transform
), tb_final AS (
    SELECT
        "key",
        dt_first_purchase,
        dt_last_purchase,
        nr_days_since_last_purchase,
        qt_total_orders,
        vl_avg_ticket,
        vl_total_spent,
        vl_avg_review_score,
        CURRENT_TIMESTAMP::TIMESTAMP AS processing_date
    FROM key_build
)
SELECT *
FROM tb_final