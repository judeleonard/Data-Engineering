{{ config(tags=['silver', 'ai']) }}
{#
  call_id is the reliable join key for call grades (lead_id is null in the source).
  We resolve lead_id/agent_id through the call log and keep BOTH the original and resolved values,
  so lineage is never lost and link quality reflects what we could actually prove.
#}
with e as (
    select * from {{ source('bronze', 'ai_events') }} where event_type = 'victoria_call_grade'
),
typed as (
    select
        event_id,
        event_type,
        {{ parse_ts('event_timestamp') }}                               as event_at,
        try_cast(json_extract_string(payload, '$.lead_id')  as bigint)  as src_lead_id,
        try_cast(json_extract_string(payload, '$.agent_id') as integer) as src_agent_id,
        try_cast(json_extract_string(payload, '$.call_id')  as bigint)  as call_id,
        try_cast(json_extract_string(payload, '$.confidence_score') as double) as confidence_score,
        json_extract_string(payload, '$.model_version')                 as model_version,
        try_cast(json_extract_string(payload, '$.sentiment_score')  as double) as sentiment_score,
        try_cast(json_extract_string(payload, '$.compliance_score') as double) as compliance_score,
        json_extract_string(payload, '$.intent')                        as intent,
        _batch_id, _ingested_at
    from e
),
dedup as (
    select * from typed
    qualify row_number() over (partition by event_id order by _ingested_at desc) = 1
)
select
    d.event_id, d.event_type, d.event_at,
    coalesce(d.src_lead_id,  c.lead_id)                                  as lead_id,
    coalesce(d.src_agent_id, c.agent_id)                                 as agent_id,
    d.call_id,
    (c.call_id is not null)                                              as call_found,
    d.confidence_score, d.model_version,
    d.sentiment_score, d.compliance_score, d.intent,
    d._batch_id, d._ingested_at,
    {{ link_quality('coalesce(d.src_lead_id, c.lead_id)', 'coalesce(d.src_agent_id, c.agent_id)') }} as link_quality
from dedup d
left join {{ ref('stg_calls') }} c using (call_id)
