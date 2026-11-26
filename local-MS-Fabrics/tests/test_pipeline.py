import duckdb
import pytest


def con(data):
    return duckdb.connect(str(data / "lakehouse.duckdb"), read_only=True)


def test_incremental_adds_only_new_rows_and_no_duplicates(lakehouse):
    data, c1 = lakehouse
    c = con(data)
    calls, uniq = c.execute("select count(*), count(distinct call_id) from gold.fact_call_attempts").fetchone()
    assert calls == uniq and calls > c1["fact_call_attempts"]
    ev, uev = c.execute("select count(*), count(distinct event_id) from gold.fact_ai_events").fetchone()
    assert ev == uev and ev > c1["fact_ai_events"]
    assert c.execute("select count(*) from gold.dim_leads").fetchone()[0] == c1["dim_leads"] + 300


def test_legacy_file_is_cleaned(lakehouse):
    data, _ = lakehouse
    c = con(data)
    # duplicate extract rows collapsed to one row per lead
    assert c.execute("select count(*) - count(distinct lead_id) from gold.dim_leads").fetchone()[0] == 0
    # 'Ontario' style names normalised to codes; no unparsed timestamps
    assert c.execute("select count(*) from gold.dim_leads where state_code is null or lead_created_at is null").fetchone()[0] == 0
    # raw phone numbers never reach gold
    cols = [r[0] for r in c.execute("describe gold.dim_leads").fetchall()]
    assert "phone" not in cols and "phone_hash" in cols


def test_business_rules_are_rederived(lakehouse):
    data, _ = lakehouse
    c = con(data)
    assert c.execute("select count(*) from gold.dim_leads where is_country_state_mismatch").fetchone()[0] > 0
    types = {r[0] for r in c.execute("select distinct revenue_type from gold.fact_sales").fetchall()}
    assert {"sale", "disposition_fee"} <= types


def test_ai_events_are_kept_and_link_quality_is_explicit(lakehouse):
    data, _ = lakehouse
    c = con(data)
    got = {r[0] for r in c.execute("select distinct link_quality from gold.fact_ai_events").fetchall()}
    assert {"full", "lead_only"} <= got and "orphan" in got      # nulls are flagged, never dropped
    # call grades resolved to a lead through the call log
    assert c.execute("""select count(*) from gold.fact_ai_events
                        where event_type='victoria_call_grade' and call_linked and lead_id is not null""").fetchone()[0] > 0


def test_scd2_snapshot_tracks_status_changes(lakehouse):
    data, _ = lakehouse
    assert con(data).execute("select count(*) from snapshots.snap_lead_state where dbt_valid_to is not null").fetchone()[0] > 0


def test_gold_parquet_exported(lakehouse):
    data, _ = lakehouse
    for t in ("fact_call_attempts", "agg_lead_funnel_daily", "dq_call_reconciliation"):
        assert (data / "gold" / f"{t}.parquet").exists()


def test_clickhouse_ddl_and_loader_sql_against_embedded_engine(lakehouse, tmp_path):
    chdb = pytest.importorskip("chdb", reason="pip install chdb to validate ClickHouse SQL without Docker")
    from chdb import session
    import load_clickhouse as lc
    data, _ = lakehouse
    lc.CH_GOLD_DIR = str(data / "gold")
    s = session.Session(str(tmp_path / "ch"))
    for stmt in [x.strip() for x in (lc.ROOT / "clickhouse" / "init.sql").read_text().split(";") if x.strip()]:
        s.query(stmt)
    rows = lambda sql: [l.split("\t") for l in str(s.query(sql, "TSV")).strip().splitlines()]
    for t in lc.TABLES:
        pcols = [r[0] for r in duckdb.execute(f"describe select * from '{data / 'gold' / (t + '.parquet')}'").fetchall()]
        ccols = [r[0] for r in rows(f"select name from system.columns where database='helpgrid' and table='{t}'")]
        s.query(lc.build_insert(t, pcols, ccols, None))
        expected = duckdb.execute(f"select count(*) from '{data / 'gold' / (t + '.parquet')}'").fetchone()[0]
        assert int(rows(f"select count() from helpgrid.{t}")[0][0]) == expected, t
