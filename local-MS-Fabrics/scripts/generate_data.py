"""Generate synthetic HelpGrid-style source data (landing zone).

Produces three files per batch in data/landing/<batch>/:
  1. HG_Legacy_Lead_Analytics_Synthetic.csv  - messy legacy extract (one row per lead, aggregated)
  2. ai_interactions_events.json             - heterogeneous AI events (katie / alas / victoria)
  3. telephony_calls.csv                     - per-dial call log (NOT in the original assessment;
                                               synthesised so call-level modelling can be demonstrated)

Deliberate data problems injected (so the pipeline has something to fix):
  - inconsistent timestamp formats, trailing space in a header, float-formatted IDs
  - duplicate lead rows across re-extracts, blank agent names with populated agent IDs
  - country/state mismatches, sale amount without sale flag, aggregates that disagree with call log
  - AI events with null lead_id / agent_id, orphan call_ids
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd

LEGACY_COLS = [
    "Lead ID", "Offer Name ", "Niche", "Lead Create TimeStamp", "Lead Opened TimeStamp",
    "Last Call TimeStamp", "Lead Country", "State", "Phone", "Agent", "Agent ID",
    "Calls Attempted", "Calls Contacted", "Last Call Outcome", "Lead Resulted in Sale",
    "Sale Amount", "HG Revenue", "DuplicateLead", "Mismatch", "Inbox Name", "Status",
    "Lead Source", "Could Call When Created", "Proper Time to Call Start in EST",
    "Proper Time to Call End in EST", "Lead Calls Schedule Start Time",
    "Lead Calls Schedule End Time", "ALAS Queue", "Lead Position", "Division",
]
US_STATES = ["CA", "NY", "TX", "FL", "OH", "PA", "IL", "GA", "NC", "MI", "WA", "AZ"]
CA_PROVINCES = ["ON", "QC", "BC", "AB", "MB", "Ontario", "Quebec"]
AGENTS = {301: "John D", 302: "Maria L", 303: "Ray Santos", 304: "Priya K", 305: "Tom W",
          306: "Elena R", 307: "Sam B", 308: "Nina P", 309: "Luis G", 310: "Dana F"}
OFFERS = [("Balance Pro", "Joint"), ("Balance Pro", "Heart"), ("Joint Restore", "Brain"),
          ("Joint Restore", "Heart")]
QUEUES = ["Green", "Maroon", "Blue"]
MODEL_VERSIONS = ["v1.2", "v1.3", "v2.0"]


def build(args):
    rng = np.random.default_rng(args.seed)
    n = args.leads
    start = pd.Timestamp(args.start_date)
    ids = np.arange(args.lead_id_start, args.lead_id_start + n)

    created = start + pd.to_timedelta(rng.integers(0, args.days * 86400, n), unit="s")
    opened = created + pd.to_timedelta(rng.integers(0, 3600, n), unit="s")
    offer_idx = rng.integers(0, len(OFFERS), n)
    country = np.where(rng.random(n) < 0.8, "US", "CA")
    state = np.where(country == "US", rng.choice(US_STATES, n), rng.choice(CA_PROVINCES, n))
    swap = rng.random(n) < 0.05  # inject country/state mismatches
    state = np.where(swap & (country == "US"), "Ontario", state)
    state = np.where(swap & (country == "CA"), "TX", state)

    # ---- call log (source of truth for attempts) -------------------------------------------
    attempts = rng.choice(np.arange(0, 9), n, p=[.12, .12, .16, .16, .14, .12, .09, .06, .03])
    agent_ids = np.array(list(AGENTS))
    lead_agent = rng.choice(agent_ids, n)
    lead_queue = rng.choice(QUEUES, n)
    calls = []
    call_id = args.call_id_start
    first_gap = rng.integers(1, 90, n)
    for i in range(n):
        t = created[i] + pd.Timedelta(minutes=int(first_gap[i]))
        for _ in range(attempts[i]):
            outcome = rng.choice(["No Answer", "Voicemail", "Contacted", "Busy"], p=[.35, .25, .28, .12])
            dur = int(rng.integers(5, 600)) if outcome == "Contacted" else int(rng.integers(0, 40))
            calls.append((call_id, ids[i], lead_agent[i], lead_queue[i], t, dur, outcome))
            call_id += 1
            t = t + pd.Timedelta(minutes=int(rng.integers(30, 600)))
    calls_df = pd.DataFrame(calls, columns=["call_id", "lead_id", "agent_id", "queue", "started_at",
                                            "duration_sec", "outcome"])

    calls_df["is_c"] = (calls_df.outcome == "Contacted").astype(int)
    g = calls_df.groupby("lead_id")
    att = g.size().reindex(ids, fill_value=0).to_numpy()
    con = g["is_c"].sum().reindex(ids, fill_value=0).to_numpy()
    last_ts = g["started_at"].max().reindex(ids)
    last_out = calls_df.sort_values("started_at").groupby("lead_id")["outcome"].last().reindex(ids)
    calls_df = calls_df.drop(columns="is_c")

    # ---- revenue -----------------------------------------------------------------------------
    sale = ((con > 0) & (rng.random(n) < 0.30)).astype(int)
    sale_amt = np.where(sale == 1, rng.choice([69.99, 89.99, 119.99], n), 0.0)
    anomalous = (sale == 0) & (rng.random(n) < 0.015)       # amount without sale flag
    sale_amt = np.where(anomalous, rng.choice([69.99, 89.99, 119.99], n), sale_amt)
    hg_rev = np.where(sale == 1, rng.choice([15.0, 20.0, 25.0], n),
                      np.where(rng.random(n) < 0.30, rng.choice([15.0, 20.0], n), 0.0))

    # ---- legacy aggregates with injected drift (2% disagree with call log) -------------------
    drift = rng.random(n) < 0.02
    legacy_att = np.where(drift, np.maximum(att + rng.choice([-1, 1], n), 0), att)
    assigned = att > 0
    agent_name = np.array([AGENTS[a] for a in lead_agent], dtype=object)
    agent_name[rng.random(n) < 0.08] = ""                   # blank name, ID still present
    agent_col = np.where(assigned, agent_name, "")
    agent_id_col = np.where(assigned, [f"{a}.0" for a in lead_agent], "")

    hour = created.hour + created.minute / 60
    could_call = ((hour >= 9) & (hour <= 21.5)).astype(int)
    override = rng.random(n) < 0.10
    sched_start = np.where(override, "10:00:00", "09:00:00")
    sched_end = np.where(override, "20:00:00", "21:30:00")

    def fmt(s, f):
        s = pd.Series(s)
        return s.dt.strftime(f).where(s.notna(), "").to_numpy()

    df = pd.DataFrame({
        "Lead ID": ids,
        "Offer Name ": [OFFERS[i][0] for i in offer_idx],
        "Niche": [OFFERS[i][1] for i in offer_idx],
        "Lead Create TimeStamp": fmt(created, "%Y-%m-%d %H:%M:%S"),
        "Lead Opened TimeStamp": fmt(opened, "%Y/%m/%d %H:%M"),
        "Last Call TimeStamp": fmt(last_ts.to_numpy(), "%m-%d-%Y %H:%M"),
        "Lead Country": country, "State": state,
        "Phone": rng.integers(2_000_000_000, 9_999_999_999, n),
        "Agent": agent_col, "Agent ID": agent_id_col,
        "Calls Attempted": legacy_att, "Calls Contacted": con,
        "Last Call Outcome": last_out.fillna("").to_numpy(),
        "Lead Resulted in Sale": sale, "Sale Amount": sale_amt, "HG Revenue": hg_rev,
        "DuplicateLead": (rng.random(n) < 0.03).astype(int),
        "Mismatch": (swap ^ (rng.random(n) < 0.03)).astype(int),   # flag mostly, not perfectly, right
        "Inbox Name": rng.choice(["Reactivation", "Recovery", "Welcome Calls"], n),
        "Status": rng.choice(["Active", "Paused", "Closed"], n, p=[.55, .3, .15]),
        "Lead Source": rng.choice(["Manual", "CheckoutChamp", "Cartpanda"], n),
        "Could Call When Created": could_call,
        "Proper Time to Call Start in EST": "09:00:00", "Proper Time to Call End in EST": "21:30:00",
        "Lead Calls Schedule Start Time": sched_start, "Lead Calls Schedule End Time": sched_end,
        "ALAS Queue": lead_queue, "Lead Position": rng.integers(1, 61, n),
        "Division": rng.choice(["WC", "ACR"], n),
    })[LEGACY_COLS]

    # ---- re-extract rows: status updates of previously seen leads + in-file duplicates -------
    extra = []
    if args.updates_from and Path(args.updates_from).exists():
        prev = pd.read_csv(args.updates_from, dtype=str, keep_default_na=False)
        prev = prev.drop_duplicates("Lead ID")
        upd = prev.sample(frac=args.update_frac, random_state=args.seed).copy()
        upd["Status"] = rng.choice(["Active", "Paused", "Closed"], len(upd))
        upd["Lead Position"] = rng.integers(1, 61, len(upd)).astype(str)
        extra.append(upd)
    extra.append(df.sample(frac=0.015, random_state=args.seed))
    df = pd.concat([df, *extra], ignore_index=True)

    # ---- AI events ---------------------------------------------------------------------------
    events = []
    eid = args.event_id_start
    ts = lambda d: d.strftime("%Y-%m-%dT%H:%M:%S")
    nullify = lambda v, p: None if rng.random() < p else v

    for i in rng.choice(n, int(n * 0.30), replace=False):          # katie_email
        events.append({"event_id": f"AI_{eid}", "event_type": "katie_email",
                       "event_timestamp": ts(created[i] + pd.Timedelta(minutes=int(rng.integers(1, 2000)))),
                       "lead_id": int(ids[i]), "agent_id": None,
                       "confidence_score": round(float(rng.beta(6, 2)), 2),
                       "model_version": str(rng.choice(MODEL_VERSIONS)),
                       "action": str(rng.choice(["draft", "send", "escalate"], p=[.6, .3, .1])),
                       "email_category": str(rng.choice(["order_status", "refund", "billing", "product_info"])),
                       "response_time_sec": int(rng.gamma(3, 70))})
        eid += 1
    for i in rng.choice(n, int(n * 0.50), replace=False):          # alas_routing_decision
        events.append({"event_id": f"AI_{eid}", "event_type": "alas_routing_decision",
                       "event_timestamp": ts(created[i] + pd.Timedelta(minutes=int(rng.integers(0, 600)))),
                       "lead_id": nullify(int(ids[i]), .45),
                       "agent_id": nullify(int(lead_agent[i]), .50),
                       "confidence_score": round(float(rng.beta(5, 3)), 2),
                       "model_version": str(rng.choice(MODEL_VERSIONS)),
                       "queue": str(lead_queue[i]), "lead_position": int(rng.integers(1, 61)),
                       "routing_reason": str(rng.choice(["agent_availability", "conversion_score", "offer_priority"]))})
        eid += 1
    contacted = calls_df[calls_df.outcome == "Contacted"]
    if len(contacted):                                              # victoria_call_grade
        graded = contacted.sample(frac=0.6, random_state=args.seed)
        for r in graded.itertuples():
            orphan = rng.random() < 0.03
            events.append({"event_id": f"AI_{eid}", "event_type": "victoria_call_grade",
                           "event_timestamp": ts(r.started_at + pd.Timedelta(minutes=int(r.duration_sec // 60) + 5)),
                           "lead_id": None, "agent_id": nullify(int(r.agent_id), .05),
                           "confidence_score": round(float(rng.beta(7, 2)), 2),
                           "model_version": str(rng.choice(MODEL_VERSIONS)),
                           "call_id": int(r.call_id) + (10_000_000 if orphan else 0),
                           "sentiment_score": round(float(rng.uniform(-1, 1)), 2),
                           "compliance_score": round(float(rng.beta(8, 2)), 2),
                           "intent": str(rng.choice(["status", "purchase", "cancel", "complaint"]))})
            eid += 1
    order = rng.permutation(len(events))
    events = [events[i] for i in order]

    calls_out = calls_df.copy()
    calls_out["started_at"] = calls_out["started_at"].dt.strftime("%Y-%m-%dT%H:%M:%S")
    return {"leads": df, "events": events, "calls": calls_out}


def main():
    p = argparse.ArgumentParser(description="Generate synthetic HelpGrid landing data")
    p.add_argument("--leads", type=int, default=50_000)
    p.add_argument("--days", type=int, default=14)
    p.add_argument("--start-date", default="2025-12-01")
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--batch", default="batch_001")
    p.add_argument("--lead-id-start", type=int, default=40_000_000)
    p.add_argument("--call-id-start", type=int, default=9_000_000)
    p.add_argument("--event-id-start", type=int, default=10_000)
    p.add_argument("--updates-from", default=None, help="prior legacy CSV; a fraction is re-emitted with changed status")
    p.add_argument("--update-frac", type=float, default=0.05)
    p.add_argument("--out", default="data/landing")
    args = p.parse_args()

    out = Path(args.out) / args.batch
    out.mkdir(parents=True, exist_ok=True)
    d = build(args)
    d["leads"].to_csv(out / "HG_Legacy_Lead_Analytics_Synthetic.csv", index=False)
    (out / "ai_interactions_events.json").write_text(json.dumps(d["events"]))
    d["calls"].to_csv(out / "telephony_calls.csv", index=False)
    print(f"[generate] {args.batch}: leads={len(d['leads']):,} events={len(d['events']):,} "
          f"calls={len(d['calls']):,} -> {out}")


if __name__ == "__main__":
    main()
