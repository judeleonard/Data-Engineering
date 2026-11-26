{#
  Grain: one row per lead that carries ANY revenue signal in the legacy file.
  The legacy file has one amount per lead, so sale_id = lead_id. revenue_type makes the three
  situations explicit instead of silently mixing them:
    sale              : sale flag = 1
    disposition_fee   : HG revenue > 0 but no sale (HG earns per-disposition) - needs business confirmation
    amount_without_sale: sale amount > 0 with sale flag = 0 - treated as a data-quality defect
#}
with last_contact as (
    select lead_id,
           arg_max(call_id, started_at) as call_id,
           max(started_at)              as last_contact_at
    from {{ ref('stg_calls') }}
    where is_contacted
    group by lead_id
)
select
    l.lead_id                                   as sale_id,
    l.lead_id,
    l.agent_id,
    lc.call_id,
    case when l.legacy_sale_flag                      then 'sale'
         when coalesce(l.sale_amount, 0) > 0          then 'amount_without_sale'
         else 'disposition_fee' end                   as revenue_type,
    coalesce(l.sale_amount, 0)                        as sale_amount,
    coalesce(l.hg_revenue, 0)                         as hg_revenue,
    coalesce(lc.last_contact_at, l.last_call_at, l.lead_created_at) as revenue_at,
    cast(coalesce(lc.last_contact_at, l.last_call_at, l.lead_created_at) as date) as revenue_date,
    l._ingested_at
from {{ ref('stg_legacy_leads') }} l
left join last_contact lc using (lead_id)
where l.legacy_sale_flag or coalesce(l.sale_amount, 0) > 0 or coalesce(l.hg_revenue, 0) > 0
