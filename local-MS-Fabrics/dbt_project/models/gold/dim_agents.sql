{# Grain: one row per agent. agent_id is canonical; the display name is the most frequent non-blank name seen. #}
with seen as (
    select agent_id, agent_name, lead_created_at as seen_at from {{ ref('stg_legacy_leads') }} where agent_id is not null
    union all
    select agent_id, null, started_at from {{ ref('stg_calls') }} where agent_id is not null
    union all
    select agent_id, null, event_at from {{ ref('stg_ai_alas_routing') }} where agent_id is not null
)
select
    agent_id,
    coalesce(mode(agent_name), 'Unknown (' || agent_id || ')') as agent_name,
    min(seen_at)                                                as first_seen_at,
    max(seen_at)                                                as last_seen_at
from seen
group by agent_id
