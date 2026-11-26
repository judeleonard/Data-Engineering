{{ config(tags=['silver', 'ai']) }}
with e as (
    select * from {{ source('bronze', 'ai_events') }} where event_type = 'katie_email'
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
        json_extract_string(payload, '$.action')                        as action,
        json_extract_string(payload, '$.email_category')                as email_category,
        try_cast(json_extract_string(payload, '$.response_time_sec') as integer) as response_time_sec,
        _batch_id, _ingested_at
    from e
)
select *, {{ link_quality('lead_id', 'agent_id') }} as link_quality
from typed
qualify row_number() over (partition by event_id order by _ingested_at desc) = 1
