{% snapshot snap_lead_state %}
{{ config(
    unique_key='lead_id',
    strategy='check',
    check_cols=['status', 'inbox_name', 'alas_queue', 'agent_id'],
    target_schema='snapshots'
) }}
{# SCD Type 2 over the cleaned lead state: every status / queue / agent change keeps its history. #}
select lead_id, status, inbox_name, alas_queue, agent_id, _ingested_at
from {{ ref('stg_legacy_leads') }}
{% endsnapshot %}
