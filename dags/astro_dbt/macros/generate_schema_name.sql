{% macro generate_schema_name(custom_schema_name, node) %}
  {%- if custom_schema_name is not none and custom_schema_name != 'undefined' -%}
    {{ custom_schema_name }}
  {%- else -%}
    {{ target.schema }}
  {%- endif -%}
{% endmacro %}