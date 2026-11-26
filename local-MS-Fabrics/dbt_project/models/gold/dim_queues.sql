{# Grain: one row per ALAS queue. Decodes colour names into a governed lookup. #}
select
    md5(lower(queue_name)) as queue_id,
    queue_name,
    priority_tier,
    description
from {{ ref('ref_queues') }}
