{#
  Grain: one row per lead where the legacy aggregate "Calls Attempted/Contacted" disagrees with the call log.
  This is how we prove (not assume) the legacy aggregates can be replaced by call-level facts.
#}
with log as (
    select lead_id, count(*) as log_attempts, count(*) filter (where is_contacted) as log_contacts
    from {{ ref('stg_calls') }}
    group by lead_id
)
select
    l.lead_id,
    l.legacy_calls_attempted,
    coalesce(g.log_attempts, 0)   as log_attempts,
    l.legacy_calls_contacted,
    coalesce(g.log_contacts, 0)   as log_contacts,
    l.legacy_calls_attempted - coalesce(g.log_attempts, 0) as attempts_delta,
    l.legacy_calls_contacted - coalesce(g.log_contacts, 0) as contacts_delta
from {{ ref('stg_legacy_leads') }} l
left join log g using (lead_id)
where abs(l.legacy_calls_attempted - coalesce(g.log_attempts, 0)) > {{ var('call_count_tolerance') }}
   or l.legacy_calls_contacted <> coalesce(g.log_contacts, 0)
