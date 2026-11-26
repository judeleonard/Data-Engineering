{# Grain: one row per lead (current state). History of status changes lives in snapshots.snap_lead_state. #}
select
    l.lead_id,
    o.offer_id,
    l.lead_source,
    l.inbox_name,
    l.division,
    l.country,
    l.state_code,
    l.phone_hash,
    l.status,
    l.alas_queue,
    l.lead_created_at,
    l.lead_opened_at,
    l.call_window_start_est,
    l.call_window_end_est,
    l.has_schedule_override,
    l.schedule_start_est,
    l.schedule_end_est,
    l.could_call_when_created,
    l.src_duplicate_flag          as is_duplicate_source_flag,
    l.is_repeated_in_source,
    l.is_country_state_mismatch,
    l.mismatch_flag_disagrees,
    l.is_amount_without_sale,
    l._ingested_at
from {{ ref('stg_legacy_leads') }} l
left join {{ ref('dim_offers') }} o
    on o.offer_name = l.offer_name and o.niche = l.niche
