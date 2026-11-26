{# Grain: call_date x agent. Activity from the call log, quality from AI call grades, revenue from sales. #}
with calls as (
    select call_date, agent_id,
           count(*)                                   as calls,
           count(*) filter (where is_contacted)       as contacts,
           count(*) filter (where is_within_call_window = false) as calls_outside_window,
           coalesce(sum(duration_sec) filter (where is_contacted), 0) as talk_time_sec
    from {{ ref('fact_call_attempts') }}
    where agent_id is not null
    group by all
),
grades as (
    select event_date as call_date, agent_id,
           count(*)                       as graded_calls,
           round(avg(sentiment_score), 3) as avg_sentiment,
           round(avg(compliance_score), 3) as avg_compliance
    from {{ ref('fact_ai_events') }}
    where event_type = 'victoria_call_grade' and agent_id is not null
    group by all
),
sales as (
    select revenue_date as call_date, agent_id,
           count(*) filter (where revenue_type = 'sale')           as sales,
           sum(sale_amount) filter (where revenue_type = 'sale')   as sale_amount,
           sum(hg_revenue)                                         as hg_revenue
    from {{ ref('fact_sales') }}
    where agent_id is not null
    group by all
)
select
    c.call_date, c.agent_id, a.agent_name,
    c.calls, c.contacts,
    round(c.contacts * 1.0 / nullif(c.calls, 0), 4)         as contact_rate,
    c.calls_outside_window, c.talk_time_sec,
    coalesce(g.graded_calls, 0)                             as graded_calls,
    g.avg_sentiment, g.avg_compliance,
    coalesce(s.sales, 0)                                    as sales,
    coalesce(s.sale_amount, 0)::decimal(14,2)               as sale_amount,
    coalesce(s.hg_revenue, 0)::decimal(14,2)                as hg_revenue
from calls c
join {{ ref('dim_agents') }} a using (agent_id)
left join grades g using (call_date, agent_id)
left join sales  s using (call_date, agent_id)
