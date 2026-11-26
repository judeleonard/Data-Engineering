-- Fails the build if more than call_count_max_mismatch_rate of leads disagree with the call log.
with n as (select count(*) as total from {{ ref('dim_leads') }}),
     d as (select count(*) as bad from {{ ref('dq_call_reconciliation') }})
select bad, total, bad * 1.0 / total as mismatch_rate
from d, n
where bad * 1.0 / total > {{ var('call_count_max_mismatch_rate') }}
