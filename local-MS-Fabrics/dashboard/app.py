"""Local stand-in for the Power BI layer. Reads the gold marts from either:
   * Parquet via DuckDB  (default, zero infrastructure), or
   * ClickHouse          (after `make clickhouse-up && make load-clickhouse`)
All queries use plain SQL that both engines understand.
"""
import os
import sys
from pathlib import Path

import duckdb
import pandas as pd
import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))
from common import GOLD  # noqa: E402

st.set_page_config(page_title="HelpGrid analytics", layout="wide")
st.title("HelpGrid analytics (local demo)")

source = st.sidebar.radio("Serving engine", ["DuckDB over Parquet", "ClickHouse"])


@st.cache_resource
def _ch():
    import clickhouse_connect
    return clickhouse_connect.get_client(host=os.environ.get("CH_HOST", "localhost"),
                                         port=int(os.environ.get("CH_PORT", "8123")))


def table(name: str) -> str:
    return f"read_parquet('{GOLD / (name + '.parquet')}')" if source.startswith("DuckDB") else f"helpgrid.{name}"


@st.cache_data(show_spinner=False, ttl=60)
def q(sql: str, engine: str) -> pd.DataFrame:
    if engine.startswith("DuckDB"):
        return duckdb.execute(sql).df()
    return _ch().query_df(sql)


try:
    funnel = q(f"""SELECT lead_date, offer_name, sum(leads) leads, sum(leads_attempted) attempted,
                          sum(leads_contacted) contacted, sum(leads_sold) sold,
                          sum(sale_amount) sale_amount, sum(hg_revenue) hg_revenue
                   FROM {table('agg_lead_funnel_daily')} GROUP BY lead_date, offer_name ORDER BY lead_date""", source)
except Exception as e:  # noqa: BLE001
    st.error(f"Could not read data from {source}: {e}\n\nRun `make pipeline` first (and `make load-clickhouse` for ClickHouse).")
    st.stop()

offers = sorted(funnel.offer_name.unique())
sel = st.sidebar.multiselect("Offer", offers, default=offers)
f = funnel[funnel.offer_name.isin(sel)]

c1, c2, c3, c4 = st.columns(4)
c1.metric("Leads", f"{int(f.leads.sum()):,}")
c2.metric("Contact rate", f"{f.contacted.sum() / max(f.leads.sum(), 1):.1%}")
c3.metric("Conversion", f"{f.sold.sum() / max(f.leads.sum(), 1):.1%}")
c4.metric("HG revenue", f"${float(f.hg_revenue.sum()):,.0f}")

st.subheader("Lead funnel by creation date")
st.bar_chart(f.groupby("lead_date")[["leads", "attempted", "contacted", "sold"]].sum())

left, right = st.columns(2)
with left:
    st.subheader("Agent leaderboard")
    agents = q(f"""SELECT agent_name, sum(calls) calls, sum(contacts) contacts,
                          sum(contacts) * 1.0 / sum(calls) contact_rate, sum(sales) sales,
                          sum(hg_revenue) hg_revenue, avg(avg_compliance) avg_compliance,
                          sum(calls_outside_window) calls_outside_window
                   FROM {table('agg_agent_performance_daily')} GROUP BY agent_name ORDER BY hg_revenue DESC""", source)
    st.dataframe(agents, width='stretch', hide_index=True)
with right:
    st.subheader("Contact rate by hour of day (attempt 1-3)")
    hr = q(f"""SELECT call_hour, attempt_number, sum(contacts) * 1.0 / sum(calls) contact_rate
               FROM {table('agg_contact_rate_by_hour')} WHERE attempt_number <= 3
               GROUP BY call_hour, attempt_number ORDER BY call_hour""", source)
    st.line_chart(hr.pivot(index="call_hour", columns="attempt_number", values="contact_rate"))

st.subheader("AI systems: model versions")
ai = q(f"""SELECT event_type, model_version, sum(events) events, avg(avg_confidence) avg_confidence,
                  sum(orphan_events) orphan_events, sum(call_not_found_events) call_not_found
           FROM {table('agg_ai_model_performance')} GROUP BY event_type, model_version
           ORDER BY event_type, model_version""", source)
st.dataframe(ai, width='stretch', hide_index=True)

with st.expander("Data quality: legacy aggregates vs call log"):
    dq = q(f"SELECT count(*) leads_disagreeing, sum(abs(attempts_delta)) total_attempt_delta FROM {table('dq_call_reconciliation')}"
           if source.startswith("DuckDB") else "SELECT 'load dq_call_reconciliation to ClickHouse to view' AS note", source)
    st.dataframe(dq, hide_index=True)
