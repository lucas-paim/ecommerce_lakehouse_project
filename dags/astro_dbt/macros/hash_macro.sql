{% macro hash_macro(source_rel, unique_key, hash_col='hc_row_hash', source_alias='s', target_alias='t') %}
  {% if is_incremental() %}
    select {{ source_alias }}.*
    from {{ source_rel }} {{ source_alias }}
    left join {{ this }} {{ target_alias }}
      on
      {% if unique_key is string %}
        {{ target_alias }}.{{ unique_key }} = {{ source_alias }}.{{ unique_key }}
      {% else %}
        {% for k in unique_key %}
          {{ target_alias }}.{{ k }} = {{ source_alias }}.{{ k }}{% if not loop.last %} and {% endif %}
        {% endfor %}
      {% endif %}
    where {{ target_alias }}.{{ hash_col }} is null
       or {{ target_alias }}.{{ hash_col }} <> {{ source_alias }}.{{ hash_col }}
  {% else %}
    select * from {{ source_rel }}
  {% endif %}
{% endmacro %}