{% macro get_current_date() %}
    {{ return("CURRENT_TIMESTAMP") }}
{% endmacro %}