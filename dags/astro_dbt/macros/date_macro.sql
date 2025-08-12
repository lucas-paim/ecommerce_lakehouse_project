{% macro date_macro() %}
{% if is_incremental() %}

WHERE 
    update_at >= (SELECT COALESCE(max(update_at), '1900-01-01'::TIMESTAMP) from {{ this }})

{% endif %}
{% endmacro %}