{# Grain: call_hour x attempt_number x within_window. Input for routing / scheduling optimisation. #}
select
    call_hour,
    least(attempt_number, 8)                                   as attempt_number,
    coalesce(is_within_call_window, false)                     as is_within_call_window,
    count(*)                                                   as calls,
    count(*) filter (where is_contacted)                       as contacts,
    round(count(*) filter (where is_contacted) * 1.0 / count(*), 4) as contact_rate
from {{ ref('fact_call_attempts') }}
group by all
