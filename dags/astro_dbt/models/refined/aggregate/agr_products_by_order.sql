{{
    config(
        materialized = "table"
    )
}}

WITH tb_order_items AS (
    SELECT
        id_order,
        id_order_item,
        id_product,
        id_seller,
        vl_price,
        vl_freight_value
    FROM {{ ref('tru_order_items') }}
), tb_orders AS (
    SELECT
        id_order,
        dh_order_purchase,
        dh_order_approved_at,
        dh_order_delivered_carrier,
        dh_order_delivered_customer,
        dh_order_estimated_delivery,
        fl_order_status
    FROM {{ ref('tru_orders') }}
), tb_products AS (
    SELECT
        id_product,
        tp_product_category                 
    FROM {{ ref('tru_products') }}   
), tb_sellers AS (
    SELECT
        id_seller,
        abbr_seller_state,
        nm_seller_city
    FROM {{ ref('tru_sellers') }}      
), join_tables AS (
    SELECT
        o.id_order,
        o.dh_order_delivered_customer,
        o.dh_order_purchase,
        o.dh_order_delivered_carrier,
        o.dh_order_approved_at,
        o.dh_order_estimated_delivery,
        o.fl_order_status,
        oi.id_order_item,
        oi.vl_price,
        oi.vl_freight_value,
        p.id_product,
        p.tp_product_category,
        s.id_seller,
        s.nm_seller_city,
        s.abbr_seller_state
    FROM tb_order_items AS oi
    LEFT JOIN tb_orders AS o
        ON
        oi.id_order = o.id_order
    LEFT JOIN tb_products AS p 
        ON
        oi.id_product = p.id_product
    LEFT JOIN tb_sellers AS s
        ON
        oi.id_seller = s.id_seller
), data_transform AS (
    SELECT
        id_order,
        id_order_item,
        id_product,
        id_seller,
        tp_product_category,
        nm_seller_city,
        abbr_seller_state,
        fl_order_status,
        (dh_order_delivered_customer - dh_order_purchase) AS nr_days_delivery,
        (vl_price + vl_freight_value) AS vl_total_price,
        (dh_order_delivered_carrier - dh_order_approved_at) AS nr_days_to_ship,
        (vl_price - vl_freight_value) AS vl_net_revenue,
        CASE
          WHEN dh_order_delivered_customer > dh_order_estimated_delivery THEN 1
          ELSE 0
        END AS fl_order_delayed
    FROM join_tables
), key_build AS (
    SELECT
        *,
        MD5(CONCAT(id_order, id_order_item)) AS "key"
    FROM data_transform
), tb_final AS (
    SELECT
        "key",
        id_order,
        id_order_item,
        id_product,
        id_seller,
        fl_order_delayed,
        fl_order_status,
        nr_days_delivery,
        nr_days_to_ship,
        abbr_seller_state,
        tp_product_category,
        vl_total_price,
        vl_net_revenue,
        nm_seller_city,
        CURRENT_TIMESTAMP::TIMESTAMP AS processing_date
    FROM key_build
)
SELECT *
FROM tb_final