-- Serving layer. Design choices:
--   * ORDER BY = the filters Power BI / dashboards use most (date first, then entity) -> sparse index prunes granules
--   * PARTITION BY month on big facts -> cheap retention (DROP PARTITION) and partition pruning
--   * LowCardinality(String) for low-cardinality text -> dictionary encoding, faster GROUP BY / smaller storage
--   * ReplacingMergeTree(_ingested_at) on incremental facts -> re-loading a batch is idempotent
--   * Pre-aggregated marts are plain MergeTree and fully reloaded (small)
CREATE DATABASE IF NOT EXISTS helpgrid;

CREATE TABLE IF NOT EXISTS helpgrid.dim_agents
(
    agent_id      Int32,
    agent_name    String,
    first_seen_at Nullable(DateTime64(6)),
    last_seen_at  Nullable(DateTime64(6))
) ENGINE = MergeTree ORDER BY agent_id;

CREATE TABLE IF NOT EXISTS helpgrid.dim_offers
(
    offer_id   String,
    offer_name LowCardinality(String),
    niche      LowCardinality(String)
) ENGINE = MergeTree ORDER BY offer_id;

CREATE TABLE IF NOT EXISTS helpgrid.dim_queues
(
    queue_id      String,
    queue_name    LowCardinality(String),
    priority_tier Int32,
    description   String
) ENGINE = MergeTree ORDER BY queue_id;

CREATE TABLE IF NOT EXISTS helpgrid.dim_leads
(
    lead_id                    Int64,
    offer_id                   String,
    lead_source                LowCardinality(String),
    inbox_name                 LowCardinality(String),
    division                   LowCardinality(String),
    country                    LowCardinality(String),
    state_code                 LowCardinality(String),
    phone_hash                 String,
    status                     LowCardinality(String),
    alas_queue                 LowCardinality(String),
    lead_created_at            DateTime64(6),
    lead_opened_at             Nullable(DateTime64(6)),
    has_schedule_override      Bool,
    could_call_when_created    Bool,
    is_duplicate_source_flag   Bool,
    is_repeated_in_source      Bool,
    is_country_state_mismatch  Bool,
    mismatch_flag_disagrees    Bool,
    is_amount_without_sale     Bool,
    _ingested_at               DateTime64(6)
) ENGINE = ReplacingMergeTree(_ingested_at)
ORDER BY lead_id;

CREATE TABLE IF NOT EXISTS helpgrid.fact_call_attempts
(
    call_id                    Int64,
    lead_id                    Int64,
    agent_id                   Nullable(Int32),
    queue_id                   Nullable(String),
    attempt_number             Int32,
    started_at                 DateTime64(6),
    call_date                  Date,
    call_hour                  UInt8,
    outcome                    LowCardinality(String),
    duration_sec               Int32,
    is_contacted               Bool,
    is_within_call_window      Nullable(Bool),
    minutes_since_lead_created Nullable(Int64),
    _ingested_at               DateTime64(6)
) ENGINE = ReplacingMergeTree(_ingested_at)
PARTITION BY toYYYYMM(call_date)
ORDER BY (call_date, lead_id, call_id);

CREATE TABLE IF NOT EXISTS helpgrid.fact_sales
(
    sale_id      Int64,
    lead_id      Int64,
    agent_id     Nullable(Int32),
    call_id      Nullable(Int64),
    revenue_type LowCardinality(String),
    sale_amount  Decimal(12, 2),
    hg_revenue   Decimal(12, 2),
    revenue_at   DateTime64(6),
    revenue_date Date,
    _ingested_at DateTime64(6)
) ENGINE = ReplacingMergeTree(_ingested_at)
ORDER BY (revenue_date, sale_id);

CREATE TABLE IF NOT EXISTS helpgrid.fact_lead_routing_snapshots
(
    snapshot_id   String,
    lead_id       Int64,
    queue_id      Nullable(String),
    lead_position Nullable(Int32),
    snapshot_at   DateTime64(6),
    trigger_event LowCardinality(String),
    model_version LowCardinality(Nullable(String)),
    routing_reason LowCardinality(Nullable(String)),
    _ingested_at  DateTime64(6)
) ENGINE = ReplacingMergeTree(_ingested_at)
PARTITION BY toYYYYMM(snapshot_at)
ORDER BY (lead_id, snapshot_at, snapshot_id);

CREATE TABLE IF NOT EXISTS helpgrid.fact_ai_events
(
    event_id          String,
    event_type        LowCardinality(String),
    event_at          DateTime64(6),
    event_date        Date,
    lead_id           Nullable(Int64),
    agent_id          Nullable(Int32),
    call_id           Nullable(Int64),
    confidence_score  Nullable(Float64),
    model_version     LowCardinality(String),
    link_quality      LowCardinality(String),
    action            LowCardinality(Nullable(String)),
    email_category    LowCardinality(Nullable(String)),
    response_time_sec Nullable(Int32),
    queue_name        LowCardinality(Nullable(String)),
    lead_position     Nullable(Int32),
    routing_reason    LowCardinality(Nullable(String)),
    sentiment_score   Nullable(Float64),
    compliance_score  Nullable(Float64),
    intent            LowCardinality(Nullable(String)),
    call_linked       Nullable(Bool),
    _ingested_at      DateTime64(6)
) ENGINE = ReplacingMergeTree(_ingested_at)
PARTITION BY toYYYYMM(event_date)
ORDER BY (event_date, event_type, event_id);

CREATE TABLE IF NOT EXISTS helpgrid.agg_lead_funnel_daily
(
    lead_date                    Date,
    offer_name                   LowCardinality(String),
    niche                        LowCardinality(String),
    lead_source                  LowCardinality(String),
    division                     LowCardinality(String),
    leads                        UInt64,
    leads_callable_on_create     UInt64,
    leads_attempted              UInt64,
    leads_contacted              UInt64,
    leads_sold                   UInt64,
    total_attempts               Int64,
    median_minutes_to_first_call Nullable(Float64),
    sale_amount                  Decimal(14, 2),
    hg_revenue                   Decimal(14, 2)
) ENGINE = MergeTree
ORDER BY (lead_date, offer_name, niche, lead_source, division);

CREATE TABLE IF NOT EXISTS helpgrid.agg_agent_performance_daily
(
    call_date            Date,
    agent_id             Int32,
    agent_name           String,
    calls                UInt64,
    contacts             UInt64,
    contact_rate         Nullable(Float64),
    calls_outside_window UInt64,
    talk_time_sec        Int64,
    graded_calls         UInt64,
    avg_sentiment        Nullable(Float64),
    avg_compliance       Nullable(Float64),
    sales                UInt64,
    sale_amount          Decimal(14, 2),
    hg_revenue           Decimal(14, 2)
) ENGINE = MergeTree
ORDER BY (call_date, agent_id);

CREATE TABLE IF NOT EXISTS helpgrid.agg_ai_model_performance
(
    event_date            Date,
    event_type            LowCardinality(String),
    model_version         LowCardinality(String),
    events                UInt64,
    avg_confidence        Nullable(Float64),
    p10_confidence        Nullable(Float64),
    p50_response_time_sec Nullable(Float64),
    p95_response_time_sec Nullable(Float64),
    orphan_events         UInt64,
    fully_linked_events   UInt64,
    call_not_found_events UInt64
) ENGINE = MergeTree
ORDER BY (event_date, event_type, model_version);

CREATE TABLE IF NOT EXISTS helpgrid.agg_contact_rate_by_hour
(
    call_hour             UInt8,
    attempt_number        Int32,
    is_within_call_window Bool,
    calls                 UInt64,
    contacts              UInt64,
    contact_rate          Float64
) ENGINE = MergeTree
ORDER BY (call_hour, attempt_number, is_within_call_window);
