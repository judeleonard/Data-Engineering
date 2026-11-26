{#
  Grain: one row per observed routing state of a lead (position in an ALAS queue at a point in time).
  Sources: (a) the legacy extract's current queue/position, (b) AI routing decisions that carry a lead_id.
  Queue-level AI decisions without a lead_id are NOT forced onto a lead; they stay in fact_ai_events.
#}
with legacy as (
    select
        lead_id, alas_queue as queue_name, lead_position,
        coalesce(last_call_at, lead_created_at) as snapshot_at,
        'legacy_extract' as trigger_event,
        cast(null as varchar) as model_version, cast(null as varchar) as routing_reason,
        schedule_start_est, schedule_end_est, _ingested_at
    from {{ ref('stg_legacy_leads') }}
    where alas_queue is not null
),
ai as (
    select
        lead_id, queue_name, lead_position, event_at as snapshot_at,
        'ai_routing_decision' as trigger_event,
        model_version, routing_reason,
        cast(null as time) as schedule_start_est, cast(null as time) as schedule_end_est, _ingested_at
    from {{ ref('stg_ai_alas_routing') }}
    where lead_id is not null
),
u as (select * from legacy union all select * from ai)
select
    md5(cast(u.lead_id as varchar) || '|' || u.trigger_event || '|' || cast(u.snapshot_at as varchar)) as snapshot_id,
    u.lead_id,
    q.queue_id,
    u.lead_position,
    u.snapshot_at,
    u.trigger_event,
    u.model_version,
    u.routing_reason,
    u.schedule_start_est,
    u.schedule_end_est,
    u._ingested_at
from u
left join {{ ref('dim_queues') }} q on q.queue_name = u.queue_name
