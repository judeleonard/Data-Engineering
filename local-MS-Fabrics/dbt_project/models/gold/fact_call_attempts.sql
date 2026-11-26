{{ config(
    materialized='incremental',
    unique_key='call_id',
    incremental_strategy='delete+insert',
    tags=['gold', 'incremental']
) }}
{#
  Grain: one row per dial attempt.
  Incremental: only calls from batches newer than what the table already holds are processed.
  attempt_number is computed in silver across ALL history, so incremental runs stay correct.
#}
select
    c.call_id,
    c.lead_id,
    c.agent_id,
    q.queue_id,
    c.attempt_number,
    c.started_at,
    cast(c.started_at as date)                           as call_date,
    extract(hour from c.started_at)::int                 as call_hour,
    c.outcome,
    c.duration_sec,
    c.is_contacted,
    -- was the dial inside the lead's permitted calling window (EST, taking overrides into account)?
    case when l.lead_id is null then null
         else cast(c.started_at as time) between coalesce(l.schedule_start_est, l.call_window_start_est)
                                             and coalesce(l.schedule_end_est,   l.call_window_end_est)
    end                                                  as is_within_call_window,
    -- minutes from lead creation to this dial (first row per lead = speed-to-lead)
    datediff('minute', l.lead_created_at, c.started_at)  as minutes_since_lead_created,
    c._ingested_at
from {{ ref('stg_calls') }} c
left join {{ ref('stg_legacy_leads') }} l using (lead_id)
left join {{ ref('dim_queues') }} q on q.queue_name = c.queue_name
{% if is_incremental() %}
where c._ingested_at > (select max(_ingested_at) from {{ this }})
{% endif %}
