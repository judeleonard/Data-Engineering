FROM quay.io/astronomer/astro-runtime:12.1.1

# setup dbt into a virtual environment
RUN python -m venv dbt_venv && source dbt_venv/bin/activate && \
    pip install --no-cache-dir dbt-postgres==1.8.2 && deactivate

# set Airflow metabase as the data warehouse for this demo
# this part is not neccessary for a production ready pipeline.
ENV AIRFLOW_CONN_AIRFLOW_METADATA_DB=postgresql+psycopg2://postgres:postgres@postgres:5432/postgres
