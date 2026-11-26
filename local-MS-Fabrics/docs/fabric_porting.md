# Porting to Microsoft Fabric

The structure transfers directly; the SQL dialect and storage format need changes.

| Concern | Local | On Fabric |
|---|---|---|
| Storage | Parquet folders | Lakehouse Files (bronze) + **Delta** tables (silver/gold) in OneLake |
| Ingestion | `ingest_bronze.py` | Data Factory Copy activity or a Spark notebook writing Delta |
| Transform engine | DuckDB via `dbt-duckdb` | `dbt-fabricspark` (Lakehouse, Spark SQL) or `dbt-fabric` (Warehouse, T-SQL) |
| Orchestration | `run_pipeline.py` | Data Factory pipeline: ingest → dbt (notebook / dbt job) → ClickHouse load |
| Incremental | `delete+insert` on `_ingested_at` | Delta `merge` strategy; consider Delta change data feed |
| Serving | ClickHouse via `file()` | Export gold to Parquet in OneLake (or use a ClickHouse OneLake/S3-compatible integration) → same DDL |
| BI | Streamlit | Power BI (Direct Lake on gold Delta, or ClickHouse connector for sub-second queries) |

## SQL rewrites (DuckDB → Spark SQL)
| DuckDB | Spark SQL |
|---|---|
| `try_strptime(x, fmt)` | `to_timestamp(x, fmt)` (returns NULL on failure in ANSI-off mode) / `try_to_timestamp` |
| `QUALIFY row_number() ... = 1` | subquery/CTE filtering on `row_number()` (Databricks has QUALIFY, Fabric Spark may not) |
| `arg_max(a, b)` | `max_by(a, b)` |
| `mode(x)` | `mode(x)` (Spark 3.4+) or a count/row_number pattern |
| `quantile_cont(x, 0.95)` | `percentile_approx(x, 0.95)` or `percentile(x, 0.95)` |
| `x::int`, `try_cast` | `cast(x as int)`, `try_cast` |
| `json_extract_string(p, '$.k')` | `get_json_object(p, '$.k')` |
| `datediff('minute', a, b)` | `(unix_timestamp(b) - unix_timestamp(a)) / 60` |
| `sha256(...)` | `sha2(..., 256)` |
| `count(*) filter (where c)` | `count_if(c)` / `sum(case when c then 1 end)` |
| `bool_or(x)` | `bool_or(x)` (Spark 3.0+) |

Keep dialect-specific functions behind macros (the project already does this for timestamp parsing and hashing: `macros/helpers.sql`), so a port touches macros rather than every model.

## Operational notes for Fabric
* Run the nightly pipeline outside Power BI refresh windows; Fabric capacity is shared, and contention shows up as queueing, not errors.
* Track per-activity duration (the local `pipeline_runs.jsonl` mimics what the Monitoring Hub shows).
* Pin the dbt adapter version and the Spark runtime.
