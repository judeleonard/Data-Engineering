"""Export gold tables from the dbt-managed DuckDB file to Parquet (the lakehouse 'gold' folder).

Files are written SORTED by their natural query key, ZSTD-compressed, with moderately sized row groups.
Sorted Parquet gives tight min/max statistics per row group, so readers (DuckDB, ClickHouse file(),
Power BI Direct Lake on Fabric) can skip most of the file for date/key filters.
"""
from __future__ import annotations

import duckdb

from common import DUCKDB_PATH, GOLD

# table -> ORDER BY used for physical sort
TABLES = {
    "dim_leads": "lead_id",
    "dim_offers": "offer_id",
    "dim_agents": "agent_id",
    "dim_queues": "queue_id",
    "fact_call_attempts": "call_date, lead_id, call_id",
    "fact_sales": "revenue_date, sale_id",
    "fact_lead_routing_snapshots": "snapshot_at, lead_id",
    "fact_ai_events": "event_date, event_type, event_id",
    "agg_lead_funnel_daily": "lead_date, offer_name, niche",
    "agg_agent_performance_daily": "call_date, agent_id",
    "agg_ai_model_performance": "event_date, event_type, model_version",
    "agg_contact_rate_by_hour": "call_hour, attempt_number",
    "dq_call_reconciliation": "lead_id",
}


def export_gold() -> dict[str, int]:
    GOLD.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(DUCKDB_PATH), read_only=True)
    counts = {}
    for table, order in TABLES.items():
        out = GOLD / f"{table}.parquet"
        con.execute(
            f"COPY (SELECT * FROM gold.{table} ORDER BY {order}) TO '{out}' "
            f"(FORMAT parquet, COMPRESSION zstd, ROW_GROUP_SIZE 122880)"
        )
        counts[table] = con.execute(f"SELECT count(*) FROM gold.{table}").fetchone()[0]
        print(f"[gold ] {table:<30} rows={counts[table]:>10,} -> {out.name}")
    return counts


if __name__ == "__main__":
    export_gold()
