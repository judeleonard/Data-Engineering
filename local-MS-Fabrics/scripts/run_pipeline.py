"""Orchestrate the nightly pipeline locally (stand-in for a Fabric Data Factory pipeline).

  ingest bronze (all landing batches not yet ingested)
    -> dbt build (silver + gold + tests + snapshot)
    -> export gold parquet
    -> [optional] load ClickHouse

Observability (this is what makes "why did the pipeline get 4x slower?" answerable):
  * every stage's wall-clock time and row counts are appended to data/ops/pipeline_runs.jsonl
  * the slowest dbt models of each run are recorded from dbt's run_results.json
  * a run is flagged if it is > --regression-factor x the median of the previous runs
"""
from __future__ import annotations

import argparse
import json
import os
import statistics
import subprocess
import sys
import time
from datetime import datetime, timezone

from common import BRONZE, DATA_DIR, GOLD, LANDING, OPS, ROOT, SILVER
from export_gold import export_gold
from ingest_bronze import ingest_batch

DBT_DIR = ROOT / "dbt_project"
LOG = OPS / "pipeline_runs.jsonl"


def run_dbt(args: list[str]) -> dict:
    env = {**os.environ, "HG_DATA_DIR": str(DATA_DIR), "DBT_SEND_ANONYMOUS_USAGE_STATS": "false"}
    cmd = ["dbt", *args, "--profiles-dir", str(DBT_DIR), "--project-dir", str(DBT_DIR)]
    proc = subprocess.run(cmd, cwd=DBT_DIR, env=env, text=True, capture_output=True)
    sys.stdout.write("\n".join(l for l in proc.stdout.splitlines()
                               if any(k in l for k in ("Done.", "ERROR", "FAIL", "WARN", "Completed")))[-3000:] + "\n")
    if proc.returncode != 0:
        sys.stdout.write(proc.stdout[-6000:])
        raise SystemExit(f"dbt {' '.join(args)} failed")
    rr = json.loads((DBT_DIR / "target" / "run_results.json").read_text())
    slow = sorted(rr["results"], key=lambda r: r["execution_time"], reverse=True)[:5]
    return {"slowest_nodes": [{"node": r["unique_id"].split(".")[-1], "sec": round(r["execution_time"], 3)} for r in slow],
            "nodes": len(rr["results"])}


def pending_batches() -> list[str]:
    if not LANDING.exists():
        return []
    done = {p.name.split("=", 1)[1] for p in (BRONZE / "legacy_leads").glob("batch=*")} if (BRONZE / "legacy_leads").exists() else set()
    return sorted(p.name for p in LANDING.iterdir() if p.is_dir() and p.name not in done)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--clickhouse", action="store_true", help="also load ClickHouse (needs docker compose up)")
    ap.add_argument("--full-refresh", action="store_true", help="rebuild incremental models from scratch")
    ap.add_argument("--regression-factor", type=float, default=2.0)
    a = ap.parse_args()

    for d in (BRONZE, SILVER, GOLD, OPS):
        d.mkdir(parents=True, exist_ok=True)

    run = {"started_at": datetime.now(timezone.utc).isoformat(timespec="seconds"), "stages": {}}
    t_all = time.perf_counter()

    def stage(name, fn):
        t = time.perf_counter()
        out = fn()
        run["stages"][name] = {"sec": round(time.perf_counter() - t, 3), **(out or {})}
        print(f"==> {name}: {run['stages'][name]['sec']}s")

    batches = pending_batches()
    print(f"pending landing batches: {batches or 'none'}")
    stage("bronze_ingest", lambda: {"batches": batches, "rows": {b: ingest_batch(b) for b in batches}})
    dbt_args = ["build"] + (["--full-refresh"] if a.full_refresh else [])
    stage("dbt_build", lambda: run_dbt(dbt_args))
    stage("gold_export", lambda: {"rows": export_gold()})
    if a.clickhouse:
        from load_clickhouse import load
        stage("clickhouse_load", lambda: {"rows": load()})

    run["total_sec"] = round(time.perf_counter() - t_all, 3)
    prev = [json.loads(l) for l in LOG.read_text().splitlines()] if LOG.exists() else []
    hist = [p["total_sec"] for p in prev[-7:] if p.get("batches_processed")]
    run["batches_processed"] = len(batches)
    if hist and batches:
        med = statistics.median(hist)
        run["vs_median"] = round(run["total_sec"] / med, 2)
        if run["total_sec"] > a.regression_factor * med:
            run["regression"] = True
            print(f"!! REGRESSION: {run['total_sec']}s vs median {med:.1f}s of last {len(hist)} runs. "
                  f"Slowest nodes: {run['stages']['dbt_build']['slowest_nodes']}")
    OPS.mkdir(parents=True, exist_ok=True)
    with LOG.open("a") as f:
        f.write(json.dumps(run) + "\n")
    print(f"\nPipeline finished in {run['total_sec']}s")


if __name__ == "__main__":
    main()
