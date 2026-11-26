"""BRONZE: land raw files as append-only, partitioned Parquet (stand-in for OneLake Files/Delta).

Rules of the bronze layer
  * NO cleaning, NO typing: CSV columns stay VARCHAR with original (messy) header names.
  * AI events keep the full original JSON in `payload` + the shared envelope as columns,
    so a brand-new event type never breaks ingestion (schema-on-read in silver).
  * Every row carries lineage: _batch_id, _source_file, _ingested_at.
  * Idempotent: a batch that is already ingested is skipped unless --force.
"""
from __future__ import annotations

import argparse
import shutil
import sys
from datetime import datetime, timezone

import duckdb

from common import BRONZE, LANDING

SOURCES = {
    # table             file                                       reader
    "legacy_leads":    ("HG_Legacy_Lead_Analytics_Synthetic.csv", "csv"),
    "telephony_calls": ("telephony_calls.csv",                    "csv"),
    "ai_events":       ("ai_interactions_events.json",            "json"),
}


def ingest_batch(batch: str, force: bool = False) -> dict[str, int]:
    src_dir = LANDING / batch
    if not src_dir.exists():
        sys.exit(f"landing batch not found: {src_dir}")
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    con = duckdb.connect()
    con.execute("SET preserve_insertion_order=false")  # faster parallel writes
    counts: dict[str, int] = {}

    for table, (fname, kind) in SOURCES.items():
        src = src_dir / fname
        out_dir = BRONZE / table / f"batch={batch}"
        if out_dir.exists():
            if not force:
                print(f"[bronze] skip {table}/{batch} (already ingested; use --force)")
                continue
            shutil.rmtree(out_dir)
        out_dir.mkdir(parents=True)
        meta = f"'{batch}' AS _batch_id, '{fname}' AS _source_file, TIMESTAMP '{now}' AS _ingested_at"

        if kind == "csv":
            # all_varchar => bronze never fails on a bad value; typing happens in silver
            query = f"""SELECT *, {meta}
                        FROM read_csv('{src}', header=true, all_varchar=true, strict_mode=false)"""
        else:
            # read_json_objects keeps each event as untouched JSON text (no auto-typing)
            query = f"""SELECT json_extract_string(json, '$.event_id')        AS event_id,
                               json_extract_string(json, '$.event_type')      AS event_type,
                               json_extract_string(json, '$.event_timestamp') AS event_timestamp,
                               json_extract_string(json, '$.lead_id')         AS lead_id,
                               json_extract_string(json, '$.agent_id')        AS agent_id,
                               json::VARCHAR AS payload, {meta}
                        FROM read_json_objects('{src}', format='array',
                                               maximum_object_size=104857600)"""
        con.execute(f"COPY ({query}) TO '{out_dir}/data.parquet' (FORMAT parquet, COMPRESSION zstd)")
        counts[table] = con.execute(f"SELECT count(*) FROM read_parquet('{out_dir}/data.parquet')").fetchone()[0]
        print(f"[bronze] {table:<16} batch={batch} rows={counts[table]:>10,}")
    return counts


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--batch", default="batch_001")
    ap.add_argument("--force", action="store_true")
    a = ap.parse_args()
    ingest_batch(a.batch, a.force)
