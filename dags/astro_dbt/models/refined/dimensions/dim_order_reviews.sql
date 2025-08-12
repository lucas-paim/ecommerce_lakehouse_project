{{
    config(
        materialized = "table",
        unique_key = "id_review"
    )
}}

WITH tb_order_reviews AS (
    SELECT
        id_review,
        id_order,
        dh_review_create,
        dh_review_answer,
        vl_review_score,
        nm_review_comment_title,
        nm_review_comment_message
    FROM {{ ref('tru_order_reviews') }}
), tb_final AS (
    SELECT
        *,
        CURRENT_TIMESTAMP::TIMESTAMP AS processing_date
    FROM tb_order_reviews
)
SELECT *
FROM tb_final