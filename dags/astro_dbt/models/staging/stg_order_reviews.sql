{{
    config(
        materialized = "table",
        dist = "review_id",
        unique_key = "review_id"
    )
}}

SELECT
    review_id::VARCHAR AS review_id,
    order_id::VARCHAR AS order_id,
    review_score::INTEGER AS review_score,
    review_comment_title::VARCHAR AS review_comment_title,
    review_comment_message::VARCHAR AS review_comment_message,
    review_creation_date:: TIMESTAMP AS review_creation_date,
    review_answer_timestamp:: TIMESTAMP AS review_answer_timestamp,
    CURRENT_TIMESTAMP::TIMESTAMP AS processing_date
FROM {{ source('raw_data', 'tb_order_reviews')}}