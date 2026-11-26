{{ config(tags=['silver', 'calls']) }}
{# Silver: one row per dial. attempt_number is computed across ALL batches so late data stays correct. #}
with src as (
    select * from {{ source('bronze', 'telephony_calls') }}
),
typed as (
    select
        try_cast(call_id as bigint)             as call_id,
        try_cast(lead_id as bigint)             as lead_id,
        try_cast(agent_id as integer)           as agent_id,
        {{ clean_str('queue') }}                as queue_name,
        {{ parse_ts('started_at') }}            as started_at,
        try_cast(duration_sec as integer)       as duration_sec,
        {{ clean_str('outcome') }}              as outcome,
        _batch_id,
        _ingested_at
    from src
),
deduped as (
    select * from typed
    where call_id is not null and lead_id is not null and started_at is not null
    qualify row_number() over (partition by call_id order by _ingested_at desc) = 1
)
select
    *,
    outcome = 'Contacted'                                                    as is_contacted,
    row_number() over (partition by lead_id order by started_at, call_id)    as attempt_number
from deduped
