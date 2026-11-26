{{ config(tags=['silver', 'ai']) }}
with e as (
    select * from {{ source('bronze', 'ai_events') }} where event_type = 'alas_routing_decision'
),
typed as (
    select
        event_id,
        event_type,
        {{ parse_ts('event_timestamp') }}                               as event_at,
        try_cast(json_extract_string(payload, '$.lead_id')  as bigint)  as lead_id,
        try_cast(json_extract_string(payload, '$.agent_id') as integer) as agent_id,
        try_cast(json_extract_string(payload, '$.confidence_score') as double) as confidence_score,
        json_extract_string(payload, '$.model_version')                 as model_version,
        json_extract_string(payload, '$.queue')                         as queue_name,
        try_cast(json_extract_string(payload, '$.lead_position') as integer) as lead_position,
        json_extract_string(payload, '$.routing_reason')                as routing_reason,
        _batch_id, _ingested_at
    from e
)
select *, {{ link_quality('lead_id', 'agent_id') }} as link_quality
from typed
qualify row_number() over (partition by event_id order by _ingested_at desc) = 1
