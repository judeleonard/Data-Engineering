-- Fails if bronze contains an AI event type that no silver model handles (a new AI system shipped events).
select distinct event_type
from {{ source('bronze', 'ai_events') }}
where event_type not in ('katie_email', 'alas_routing_decision', 'victoria_call_grade')
