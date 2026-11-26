"""Load gold Parquet into ClickHouse (the serving layer that Power BI would query).

Fast path: ClickHouse reads the Parquet files itself via file() (the data/gold folder is mounted
into the container's user_files directory) - no row-by-row transfer through Python.

  * incremental facts: only rows newer than the table's _ingested_at watermark are inserted
    (ReplacingMergeTree makes accidental re-inserts harmless)
  * everything else: TRUNCATE + reload (small dimensions / pre-aggregated marts)
Only columns present in BOTH the Parquet file and the ClickHouse table are loaded, so adding a column
in dbt never breaks the load; it simply isn't served until the DDL is extended.
"""
from __future__ import annotations

import argparse
import os
from pathlib import Path

import duckdb

from common import GOLD, ROOT

DB = "helpgrid"
INCREMENTAL = {"fact_call_attempts", "fact_ai_events"}
TABLES = [
    "dim_agents", "dim_offers", "dim_queues", "dim_leads",
    "fact_call_attempts", "fact_sales", "fact_lead_routing_snapshots", "fact_ai_events",
    "agg_lead_funnel_daily", "agg_agent_performance_daily", "agg_ai_model_performance",
    "agg_contact_rate_by_hour",
]
# folder as seen from INSIDE the ClickHouse container (see docker-compose.yml)
CH_GOLD_DIR = os.environ.get("CH_GOLD_DIR", "gold")


def _client():
    import clickhouse_connect
    return clickhouse_connect.get_client(
        host=os.environ.get("CH_HOST", "localhost"), port=int(os.environ.get("CH_PORT", "8123")),
        username=os.environ.get("CH_USER", "default"), password=os.environ.get("CH_PASSWORD", ""),
    )


def build_insert(table: str, parquet_cols: list[str], ch_cols: list[str], watermark: str | None) -> str:
    cols = [c for c in parquet_cols if c in ch_cols]
    col_list = ", ".join(f"`{c}`" for c in cols)
    where = f"WHERE _ingested_at > toDateTime64('{watermark}', 6)" if watermark else ""
    return (f"INSERT INTO {DB}.{table} ({col_list}) "
            f"SELECT {col_list} FROM file('{CH_GOLD_DIR}/{table}.parquet', 'Parquet') {where} "
            f"SETTINGS input_format_null_as_default = 1")


def load(dry_run: bool = False) -> dict[str, int]:
    ddl = (ROOT / "clickhouse" / "init.sql").read_text()
    client = None if dry_run else _client()
    if not dry_run:
        for stmt in [s.strip() for s in ddl.split(";") if s.strip()]:
            client.command(stmt)
    loaded = {}
    for t in TABLES:
        pq = GOLD / f"{t}.parquet"
        parquet_cols = [r[0] for r in duckdb.execute(f"DESCRIBE SELECT * FROM '{pq}'").fetchall()]
        if dry_run:
            ch_cols = parquet_cols
            wm = None
        else:
            ch_cols = [r[0] for r in client.query(
                f"SELECT name FROM system.columns WHERE database='{DB}' AND table='{t}'").result_rows]
            wm = None
            if t in INCREMENTAL:
                m = client.query(f"SELECT max(_ingested_at) FROM {DB}.{t}").result_rows[0][0]
                wm = m.strftime("%Y-%m-%d %H:%M:%S.%f") if m and m.year > 1970 else None
            if t not in INCREMENTAL:
                client.command(f"TRUNCATE TABLE {DB}.{t}")
        sql = build_insert(t, parquet_cols, ch_cols, wm)
        if dry_run:
            print(sql[:220] + " ...")
            continue
        client.command(sql)
        n = client.query(f"SELECT count() FROM {DB}.{t}").result_rows[0][0]
        loaded[t] = n
        print(f"[clickhouse] {t:<30} rows={n:>10,}" + (f"  (watermark {wm})" if wm else ""))
    return loaded


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true", help="print the INSERT statements without connecting")
    load(ap.parse_args().dry_run)
