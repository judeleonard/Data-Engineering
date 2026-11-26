{# Grain: one row per (offer_name, niche). Surrogate key is a stable hash so it survives reloads. #}
select distinct
    md5(lower(offer_name) || '|' || lower(niche)) as offer_id,
    offer_name,
    niche
from {{ ref('stg_legacy_leads') }}
where offer_name is not null and niche is not null
