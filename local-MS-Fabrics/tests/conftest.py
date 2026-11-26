import os
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))


@pytest.fixture(scope="session")
def lakehouse(tmp_path_factory):
    """Run the whole pipeline on a tiny dataset in an isolated temp dir: batch 1 then an incremental batch 2."""
    data = tmp_path_factory.mktemp("hg_data")
    env = {**os.environ, "HG_DATA_DIR": str(data)}
    py = sys.executable

    def sh(*args):
        r = subprocess.run([py, *args], cwd=ROOT / "scripts", env=env, text=True, capture_output=True)
        assert r.returncode == 0, r.stdout[-3000:] + r.stderr[-3000:]

    sh("generate_data.py", "--leads", "1500", "--out", str(data / "landing"))
    sh("run_pipeline.py")
    first = data / "snapshot_counts"
    import duckdb
    con = duckdb.connect(str(data / "lakehouse.duckdb"), read_only=True)
    counts1 = {t: con.execute(f"select count(*) from gold.{t}").fetchone()[0]
               for t in ("fact_call_attempts", "fact_ai_events", "dim_leads")}
    con.close()
    sh("generate_data.py", "--leads", "300", "--batch", "batch_002", "--start-date", "2025-12-20", "--seed", "9",
       "--lead-id-start", "40100000", "--call-id-start", "9500000", "--event-id-start", "500000",
       "--updates-from", str(data / "landing/batch_001/HG_Legacy_Lead_Analytics_Synthetic.csv"),
       "--out", str(data / "landing"))
    sh("run_pipeline.py")
    return data, counts1
