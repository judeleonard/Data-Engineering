-- attempt_number must run 1..n per lead with no gaps (guards the incremental logic)
select lead_id
from {{ ref('fact_call_attempts') }}
group by lead_id
having max(attempt_number) <> count(*)
