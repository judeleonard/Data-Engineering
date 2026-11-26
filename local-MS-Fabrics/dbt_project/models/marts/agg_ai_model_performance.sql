{# Grain: event_date x event_type x model_version. Drives model A/B comparison and link-quality monitoring. #}
select
    event_date, event_type, model_version,
    count(*)                                             as events,
    round(avg(confidence_score), 4)                      as avg_confidence,
    round(quantile_cont(confidence_score, 0.10), 4)      as p10_confidence,
    round(quantile_cont(response_time_sec, 0.50), 1)     as p50_response_time_sec,
    round(quantile_cont(response_time_sec, 0.95), 1)     as p95_response_time_sec,
    count(*) filter (where link_quality = 'orphan')      as orphan_events,
    count(*) filter (where link_quality = 'full')        as fully_linked_events,
    count(*) filter (where call_linked = false)          as call_not_found_events
from {{ ref('fact_ai_events') }}
group by all
