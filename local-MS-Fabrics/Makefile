# HelpGrid medallion demo. `make help` lists targets.
SHELL := /bin/bash
VENV  := .venv
PY    := $(VENV)/bin/python
export HG_DATA_DIR ?= $(CURDIR)/data
export DBT_SEND_ANONYMOUS_USAGE_STATS := false

LEADS ?= 50000

.PHONY: help install data pipeline incremental clickhouse-up load-clickhouse dashboard test report dbt-docs clean nuke

help:            ## show targets
	@grep -E '^[a-z-]+:.*##' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-16s %s\n", $$1, $$2}'

$(VENV)/bin/activate: requirements.txt
	python3 -m venv $(VENV)
	$(PY) -m pip install -q --upgrade pip
	$(PY) -m pip install -q -r requirements.txt
	touch $(VENV)/bin/activate

install: $(VENV)/bin/activate   ## create venv + install dependencies

data: install   ## generate synthetic landing data  (LEADS=50000 by default)
	$(PY) scripts/generate_data.py --leads $(LEADS) --batch batch_001

pipeline: install   ## bronze -> silver -> gold -> parquet export (processes every new landing batch)
	cd scripts && ../$(PY) run_pipeline.py

incremental: install   ## land a 2nd batch (new leads + status changes) and run the pipeline incrementally
	$(PY) scripts/generate_data.py --batch batch_002 --leads $$(( $(LEADS) / 10 )) --start-date 2025-12-15 --seed 7 \
	  --lead-id-start 40100000 --call-id-start 9500000 --event-id-start 500000 \
	  --updates-from data/landing/batch_001/HG_Legacy_Lead_Analytics_Synthetic.csv
	cd scripts && ../$(PY) run_pipeline.py

clickhouse-up:   ## start ClickHouse (needs Docker)
	docker compose up -d --wait

load-clickhouse: install   ## load gold parquet into ClickHouse
	cd scripts && ../$(PY) load_clickhouse.py

dashboard: install   ## Streamlit dashboard on http://localhost:8501
	$(PY) -m streamlit run dashboard/app.py

test: install   ## end-to-end pytest suite (isolated temp lakehouse)
	$(PY) -m pytest -q tests

report: install   ## show run history + slowest dbt nodes (pipeline observability)
	@$(PY) -c "import json,pathlib; p=pathlib.Path('$(HG_DATA_DIR)/ops/pipeline_runs.jsonl'); \
	[print(r['started_at'], f\"{r['total_sec']:>7}s\", 'vs_median=' + str(r.get('vs_median','-')), 'REGRESSION' if r.get('regression') else '', \
	 [ (n['node'], n['sec']) for n in r['stages']['dbt_build']['slowest_nodes'][:3] ]) for r in map(json.loads, p.read_text().splitlines())]"

dbt-docs: install   ## generate + serve dbt docs (lineage graph) on :8080
	cd dbt_project && ../$(VENV)/bin/dbt docs generate --profiles-dir . && ../$(VENV)/bin/dbt docs serve --profiles-dir . --port 8080

clean:   ## remove generated lakehouse layers (keeps landing files)
	rm -rf data/bronze data/silver data/gold data/ops data/lakehouse.duckdb dbt_project/target dbt_project/logs

nuke: clean   ## remove everything generated, including landing + venv
	rm -rf data/landing $(VENV)
