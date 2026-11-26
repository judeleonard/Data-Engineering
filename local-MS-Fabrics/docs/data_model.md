# Data model

```mermaid
erDiagram
  dim_offers ||--o{ dim_leads : "offer_id"
  dim_leads  ||--o{ fact_call_attempts : "lead_id"
  dim_agents ||--o{ fact_call_attempts : "agent_id"
  dim_queues ||--o{ fact_call_attempts : "queue_id"
  dim_leads  ||--o{ fact_sales : "lead_id"
  dim_leads  ||--o{ fact_lead_routing_snapshots : "lead_id"
  dim_queues ||--o{ fact_lead_routing_snapshots : "queue_id"
  dim_leads  |o--o{ fact_ai_events : "lead_id (nullable)"
  dim_agents |o--o{ fact_ai_events : "agent_id (nullable)"
  fact_call_attempts |o--o{ fact_ai_events : "call_id (nullable)"
  fact_call_attempts |o--o| fact_sales : "call_id (nullable)"

  dim_leads { bigint lead_id PK
              varchar offer_id FK
              varchar status
              varchar state_code
              varchar phone_hash
              boolean is_country_state_mismatch
              boolean has_schedule_override }
  fact_call_attempts { bigint call_id PK
              bigint lead_id FK
              int agent_id FK
              int attempt_number
              timestamp started_at
              boolean is_contacted
              boolean is_within_call_window }
  fact_sales { bigint sale_id PK
              bigint lead_id FK
              varchar revenue_type
              decimal sale_amount
              decimal hg_revenue }
  fact_ai_events { varchar event_id PK
              varchar event_type
              bigint lead_id FK
              int agent_id FK
              bigint call_id FK
              varchar model_version
              varchar link_quality }
```

Differences from the earlier design discussion, deliberately made while building it:

* `dim_leads` holds **current** state; history lives in `snapshots.snap_lead_state` (dbt snapshot) rather than `valid_from/valid_to` columns.
* `fact_sales` holds every lead with a revenue signal and labels it with `revenue_type`, instead of pretending all rows are sales.
* `fact_ai_events` adds `call_linked` and resolves `lead_id`/`agent_id` for call grades through the call log.
* `fact_call_attempts` requires a call-level source; the synthetic `telephony_calls.csv` stands in for it.
