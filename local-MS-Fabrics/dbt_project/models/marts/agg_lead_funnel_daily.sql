{# Grain: lead_date x offer x niche x source x division. Lead lifecycle + revenue funnel. #}
with per_lead as (
    select lead_id,
           count(*)                           as attempts,
           count(*) filter (where is_contacted) as contacts,
           min(minutes_since_lead_created)    as minutes_to_first_call
    from {{ ref('fact_call_attempts') }}
    group by lead_id
),
rev as (
    select lead_id,
           sum(sale_amount) filter (where revenue_type = 'sale') as sale_amount,
           sum(hg_revenue)                                       as hg_revenue,
           bool_or(revenue_type = 'sale')                        as has_sale
    from {{ ref('fact_sales') }}
    group by lead_id
)
select
    cast(l.lead_created_at as date)                        as lead_date,
    o.offer_name, o.niche, l.lead_source, l.division,
    count(*)                                               as leads,
    count(*) filter (where l.could_call_when_created)      as leads_callable_on_create,
    count(*) filter (where coalesce(p.attempts, 0) > 0)    as leads_attempted,
    count(*) filter (where coalesce(p.contacts, 0) > 0)    as leads_contacted,
    count(*) filter (where coalesce(r.has_sale, false))    as leads_sold,
    coalesce(sum(p.attempts), 0)                           as total_attempts,
    round(median(p.minutes_to_first_call), 1)              as median_minutes_to_first_call,
    coalesce(sum(r.sale_amount), 0)::decimal(14,2)         as sale_amount,
    coalesce(sum(r.hg_revenue), 0)::decimal(14,2)          as hg_revenue
from {{ ref('dim_leads') }} l
join {{ ref('dim_offers') }} o using (offer_id)
left join per_lead p using (lead_id)
left join rev r using (lead_id)
group by all
