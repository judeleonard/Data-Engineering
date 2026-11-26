{{ config(
    materialized='incremental',
    unique_key='event_id',
    incremental_strategy='delete+insert',
    tags=['gold', 'incremental']
) }}
{#
  Grain: one row per AI event (all systems). Type-specific columns are NULL for other event types.
  link_quality: full | lead_only | agent_only | orphan (computed AFTER resolving via the call log).
  call_linked: for call grades, did call_id exist in the call log? (orphans are kept, flagged, never dropped)
#}
with unioned as (
    select event_id, event_type, event_at, lead_id, agent_id, cast(null as bigint) as call_id,
           confidence_score, model_version, link_quality,
           action, email_category, response_time_sec,
           cast(null as varchar) as queue_name, cast(null as integer) as lead_position, cast(null as varchar) as routing_reason,
           cast(null as double) as sentiment_score, cast(null as double) as compliance_score, cast(null as varchar) as intent,
           cast(null as boolean) as call_linked, _ingested_at
    from {{ ref('stg_ai_katie_email') }}
    union all
    select event_id, event_type, event_at, lead_id, agent_id, null,
           confidence_score, model_version, link_quality,
           null, null, null,
           queue_name, lead_position, routing_reason,
           null, null, null,
           null, _ingested_at
    from {{ ref('stg_ai_alas_routing') }}
    union all
    select event_id, event_type, event_at, lead_id, agent_id, call_id,
           confidence_score, model_version, link_quality,
           null, null, null,
           null, null, null,
           sentiment_score, compliance_score, intent,
           call_found, _ingested_at
    from {{ ref('stg_ai_call_grades') }}
)
select
    event_id, event_type, event_at, cast(event_at as date) as event_date,
    lead_id, agent_id, call_id,
    confidence_score, model_version, link_quality,
    action, email_category, response_time_sec,
    queue_name, lead_position, routing_reason,
    sentiment_score, compliance_score, intent,
    call_linked, _ingested_at
from unioned
{% if is_incremental() %}
where _ingested_at > (select max(_ingested_at) from {{ this }})
{% endif %}
