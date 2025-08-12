{{
    config(
        materialized = 'incremental',
        incremental_strategy = 'merge',
        unique_key = 'id_review',
        on_schema_change = 'sync_all_columns',
        post_hook = [
            "CREATE UNIQUE index IF NOT EXISTS {{ this.name }}_uk on {{ this }} (id_review)"
            ]
    )
}}

WITH tb_order_reviews AS (
    SELECT
        review_id                       AS id_review,
        order_id                        AS id_order,
        review_score                    AS vl_review_score,
        review_comment_title            AS nm_review_comment_title,
        review_comment_message          AS nm_review_comment_message,
        review_creation_date            AS dh_review_create,
        review_answer_timestamp         AS dh_review_answer,
        ROW_NUMBER() OVER(
            PARTITION BY review_id
            ORDER BY review_id DESC
        )        AS index
    FROM {{ ref('stg_order_reviews') }}
), tb_order_reviews_filtered AS (
    SELECT *
    FROM tb_order_reviews
    WHERE index = 1
), hash_generation AS (
    SELECT
        id_review,
        id_order,
        vl_review_score,
        nm_review_comment_title,
        nm_review_comment_message,
        dh_review_create,
        dh_review_answer,
        MD5(
            COALESCE(vl_review_score::VARCHAR,'')             || '|' ||
            COALESCE(nm_review_comment_title::VARCHAR,'')     || '|' ||
            COALESCE(nm_review_comment_message::VARCHAR,'')   || '|' ||
            COALESCE(dh_review_create::VARCHAR,'')            || '|' ||
            COALESCE(dh_review_answer::VARCHAR,'')
        ) AS hc_row_hash,
        CURRENT_TIMESTAMP::TIMESTAMP AS processing_date
    FROM tb_order_reviews_filtered
), delta AS (
    {{ hash_macro('hash_generation', 'id_review') }}
), tb_final AS (
    SELECT
        id_review,
        id_order,
        dh_review_create,
        dh_review_answer,
        vl_review_score,
        nm_review_comment_title,
        nm_review_comment_message,
        hc_row_hash,
        processing_date
    FROM delta
)
SELECT *
FROM tb_final