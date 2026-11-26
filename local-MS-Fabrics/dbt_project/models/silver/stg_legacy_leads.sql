{{ config(tags=['silver', 'legacy']) }}
{#
  Silver: one row per lead (latest extract wins). Fixes typing, timestamp formats, blanks,
  PII, and RE-DERIVES the embedded business rules instead of trusting the legacy flags.
#}
with src as (
    select * from {{ source('bronze', 'legacy_leads') }}
),

typed as (
    select
        try_cast("Lead ID" as bigint)                                   as lead_id,
        {{ clean_str('"Offer Name"') }}                                 as offer_name,
        {{ clean_str('"Niche"') }}                                      as niche,
        {{ parse_ts('"Lead Create TimeStamp"') }}                       as lead_created_at,
        {{ parse_ts('"Lead Opened TimeStamp"') }}                       as lead_opened_at,
        {{ parse_ts('"Last Call TimeStamp"') }}                         as last_call_at,
        upper({{ clean_str('"Lead Country"') }})                        as country,
        upper({{ clean_str('"State"') }})                               as state_raw,
        {{ hash_pii('"Phone"') }}                                       as phone_hash,
        {{ clean_str('"Agent"') }}                                      as agent_name,
        try_cast(try_cast("Agent ID" as double) as integer)             as agent_id,
        try_cast("Calls Attempted" as integer)                          as legacy_calls_attempted,
        try_cast("Calls Contacted" as integer)                          as legacy_calls_contacted,
        {{ clean_str('"Last Call Outcome"') }}                          as last_call_outcome,
        try_cast("Lead Resulted in Sale" as integer) = 1                as legacy_sale_flag,
        try_cast("Sale Amount" as decimal(10,2))                        as sale_amount,
        try_cast("HG Revenue"  as decimal(10,2))                        as hg_revenue,
        try_cast("DuplicateLead" as integer) = 1                        as src_duplicate_flag,
        try_cast("Mismatch" as integer) = 1                             as src_mismatch_flag,
        {{ clean_str('"Inbox Name"') }}                                 as inbox_name,
        {{ clean_str('"Status"') }}                                     as status,
        {{ clean_str('"Lead Source"') }}                                as lead_source,
        try_cast("Could Call When Created" as integer) = 1              as src_could_call_when_created,
        try_cast("Proper Time to Call Start in EST" as time)            as proper_call_start_est,
        try_cast("Proper Time to Call End in EST" as time)              as proper_call_end_est,
        try_cast("Lead Calls Schedule Start Time" as time)              as schedule_start_est,
        try_cast("Lead Calls Schedule End Time" as time)                as schedule_end_est,
        {{ clean_str('"ALAS Queue"') }}                                 as alas_queue,
        try_cast("Lead Position" as integer)                            as lead_position,
        {{ clean_str('"Division"') }}                                   as division,
        _batch_id,
        _ingested_at
    from src
),

deduped as (
    select
        *,
        count(*) over (partition by lead_id) as source_row_count
    from typed
    where lead_id is not null
    qualify row_number() over (
        partition by lead_id
        order by _ingested_at desc, last_call_at desc nulls last
    ) = 1
),

-- State is resolved on its own (code OR full name); country consistency is checked afterwards.
-- Codes/names are unique across US + CA, so no country is needed for the lookup.
geo as (
    select country as state_country, state_code, upper(state_name) as state_name_uc from {{ ref('ref_geo') }}
)

select
    d.lead_id,
    d.offer_name,
    d.niche,
    d.lead_created_at,
    d.lead_opened_at,
    d.last_call_at,
    d.country,
    g.state_code                                                         as state_code,
    d.state_raw                                                          as state_raw,
    d.phone_hash,
    d.agent_name,
    d.agent_id,
    d.legacy_calls_attempted,
    d.legacy_calls_contacted,
    d.last_call_outcome,
    d.legacy_sale_flag,
    d.sale_amount,
    d.hg_revenue,
    d.inbox_name,
    d.status,
    d.lead_source,
    d.alas_queue,
    d.lead_position,
    d.division,

    -- ---- business rules, re-derived and compared with what the legacy file claimed ----
    d.src_duplicate_flag,
    d.source_row_count > 1                                               as is_repeated_in_source,
    d.src_mismatch_flag,
    (g.state_code is null or g.state_country <> d.country)               as is_country_state_mismatch,
    (d.src_mismatch_flag <> (g.state_code is null or g.state_country <> d.country))
                                                                         as mismatch_flag_disagrees,
    d.proper_call_start_est                                              as call_window_start_est,
    d.proper_call_end_est                                                as call_window_end_est,
    (d.schedule_start_est <> d.proper_call_start_est
        or d.schedule_end_est <> d.proper_call_end_est)                  as has_schedule_override,
    d.schedule_start_est,
    d.schedule_end_est,
    d.src_could_call_when_created,
    d.lead_created_at is not null
        and cast(d.lead_created_at as time) between d.proper_call_start_est and d.proper_call_end_est
                                                                         as could_call_when_created,
    (coalesce(d.sale_amount, 0) > 0 and not d.legacy_sale_flag)          as is_amount_without_sale,

    d._batch_id,
    d._ingested_at
from deduped d
left join geo g
    on g.state_code = d.state_raw or g.state_name_uc = d.state_raw
