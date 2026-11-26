{# Parse the several timestamp formats found in the legacy extract + ISO events. NULL if none match. #}
{% macro parse_ts(col) -%}
coalesce(
    try_strptime(trim({{ col }}), '%Y-%m-%d %H:%M:%S'),
    try_strptime(trim({{ col }}), '%Y-%m-%dT%H:%M:%S'),
    try_strptime(trim({{ col }}), '%Y/%m/%d %H:%M'),
    try_strptime(trim({{ col }}), '%m-%d-%Y %H:%M')
)
{%- endmacro %}

{# Salted SHA-256 of digits only. Raw phone numbers never leave silver-staging. #}
{% macro hash_pii(col) -%}
sha256('{{ env_var("HG_PII_SALT", "dev-only-salt") }}' || regexp_replace(cast({{ col }} as varchar), '[^0-9]', '', 'g'))
{%- endmacro %}

{# '' / whitespace -> NULL, otherwise trimmed. #}
{% macro clean_str(col) -%}
nullif(trim({{ col }}), '')
{%- endmacro %}

{# How completely an AI event could be tied to a lead/agent. Used for filtering + monitoring. #}
{% macro link_quality(lead_col, agent_col) -%}
case
    when {{ lead_col }} is not null and {{ agent_col }} is not null then 'full'
    when {{ lead_col }} is not null then 'lead_only'
    when {{ agent_col }} is not null then 'agent_only'
    else 'orphan'
end
{%- endmacro %}

