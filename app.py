import warnings
warnings.filterwarnings("ignore")

from flask import Flask, render_template, request, jsonify
from redshift_client import get_connection
import pandas as pd
from datetime import datetime, timedelta
import threading
import time as _time
import json as _json

app = Flask(__name__)

# ── Server-side cache ────────────────────────────────────────────────────────
_cache = {}
_cache_lock = threading.Lock()
CACHE_TTL = 300  # 5 minutes in seconds


def get_cached(key):
    with _cache_lock:
        entry = _cache.get(key)
        if entry and (_time.time() - entry["ts"]) < CACHE_TTL:
            return entry["data"]
    return None


def set_cached(key, data):
    with _cache_lock:
        _cache[key] = {"data": data, "ts": _time.time()}

# Scott Nicol (777488) excluded — he heads the team, not gauged
WM_USERS = {
    777489: ("Ryan Dougan", "WM"),
    777490: ("Justin James Beckwith", "WM"),
    777564: ("Alshareef Al-Shareef", "WM"),
    777567: ("Garima Bhargava", "WM"),
    777623: ("Karan Sehgal", "WM"),
}
ALL_IDS = ",".join(str(uid) for uid in WM_USERS)
NAME_MAP = {uid: n for uid, (n, _) in WM_USERS.items()}


def default_dates():
    end = datetime.today().date()
    start = end - timedelta(days=89)
    return str(start), str(end)


def _run_q(sql):
    """Run a single query on its own connection (for parallel use)."""
    conn = get_connection()
    try:
        return pd.read_sql(sql, conn)
    finally:
        conn.close()


def fetch_all(ids, start, end):
    """Run all queries in parallel using ThreadPoolExecutor."""
    from concurrent.futures import ThreadPoolExecutor, as_completed
    q = _run_q

    sqls = {
        "meetings": f"""
            SELECT m.organizer_id AS user_id, mt.key AS meeting_type
            FROM propforce_sa.propforce_sa__management_meetings m
            JOIN propforce_sa.propforce_sa__management_meeting_types mt ON m.type_id = mt.id
            WHERE (m.operation_type IS NULL OR m.operation_type != 'delete')
              AND m.organizer_id IN ({ids})
              AND m.scheduled_start_time::date BETWEEN '{start}' AND '{end}'
        """,
        "attendees": f"""
            SELECT ma.attendee_id AS user_id, mt.key AS meeting_type,
                   COUNT(DISTINCT m.id) AS cnt
            FROM propforce_sa.propforce_sa__management_meeting_attendees ma
            JOIN propforce_sa.propforce_sa__management_meetings m ON ma.meeting_id = m.id
            JOIN propforce_sa.propforce_sa__management_meeting_types mt ON m.type_id = mt.id
            WHERE (ma.operation_type IS NULL OR ma.operation_type != 'delete')
              AND (m.operation_type IS NULL OR m.operation_type != 'delete')
              AND ma.attendee_id IN ({ids})
              AND ma.present = 1
              AND m.scheduled_start_time::date BETWEEN '{start}' AND '{end}'
            GROUP BY ma.attendee_id, mt.key
        """,
        "unique_meetings": f"""
            SELECT t.assigned_to AS user_id, COUNT(DISTINCT t.task_against_id) AS cnt
            FROM propforce_sa.propforce_sa__zn_pf_task t
            JOIN propforce_sa.propforce_sa__zn_pf_task_types tt ON t.task_type_id = tt.task_type_id
            WHERE t.assigned_to IN ({ids})
              AND t.task_against_id IS NOT NULL
              AND t.date_added::date BETWEEN '{start}' AND '{end}'
              AND (tt.key ILIKE '%meeting_done%'
                   OR tt.key IN ('outdoor','company_office','site_office',
                                 'google_meet','zoom_call','whatsapp_video_call','video_meeting'))
            GROUP BY t.assigned_to
        """,
        "tasks": f"""
            SELECT t.assigned_to AS user_id, tt.title AS task_type, tt.key AS task_key,
                   COUNT(*) AS cnt
            FROM propforce_sa.propforce_sa__zn_pf_task t
            JOIN propforce_sa.propforce_sa__zn_pf_task_types tt ON t.task_type_id = tt.task_type_id
            WHERE t.assigned_to IN ({ids})
              AND t.date_added::date BETWEEN '{start}' AND '{end}'
            GROUP BY t.assigned_to, tt.title, tt.key
        """,
        "investors": f"""
            SELECT t.assigned_to AS user_id, COUNT(DISTINCT t.client_id) AS cnt
            FROM propforce_sa.propforce_sa__zn_pf_task t
            WHERE t.assigned_to IN ({ids}) AND t.client_id IS NOT NULL
              AND t.date_added::date BETWEEN '{start}' AND '{end}'
            GROUP BY t.assigned_to
        """,
        "leads": f"""
            SELECT userid AS user_id, COUNT(DISTINCT inquiry_id) AS cnt
            FROM propforce_sa.propforce_sa__zn_internal_inquiries
            WHERE userid IN ({ids})
              AND (operation_type IS NULL OR operation_type != 'delete')
              AND time_added::date BETWEEN '{start}' AND '{end}'
            GROUP BY userid
        """,
        "leads_worked_on": f"""
            SELECT t.assigned_to AS user_id, COUNT(DISTINCT t.task_against_id) AS cnt
            FROM propforce_sa.propforce_sa__zn_pf_task t
            WHERE t.assigned_to IN ({ids})
              AND t.task_against_id IS NOT NULL
              AND t.date_added::date BETWEEN '{start}' AND '{end}'
            GROUP BY t.assigned_to
        """,
        "pf_logins": f"""
            SELECT user_id, COUNT(DISTINCT date_added::date) AS cnt
            FROM propforce_sa.propforce_sa__zn_user_activities
            WHERE user_id IN ({ids})
              AND activity_type = 'login'
              AND date_added::date BETWEEN '{start}' AND '{end}'
            GROUP BY user_id
        """,
        "pf_logins_daily": f"""
            SELECT date_added::date AS day, COUNT(DISTINCT user_id) AS cnt
            FROM propforce_sa.propforce_sa__zn_user_activities
            WHERE user_id IN ({ids})
              AND activity_type = 'login'
              AND date_added::date BETWEEN '{start}' AND '{end}'
            GROUP BY day ORDER BY day
        """,
        "pf_logins_bar": f"""
            SELECT user_id, COUNT(DISTINCT date_added::date) AS cnt
            FROM propforce_sa.propforce_sa__zn_user_activities
            WHERE user_id IN ({ids})
              AND activity_type = 'login'
              AND date_added::date BETWEEN '{start}' AND '{end}'
            GROUP BY user_id ORDER BY cnt DESC
        """,
        "funnel": f"""
            SELECT user_id,
                   inquiry_clients AS inquiries, prospect_clients AS prospects,
                   mature_clients AS mature, preclosure_clients AS preclosure,
                   sold_clients AS sold, closedlost_clients AS closedlost,
                   mature_revenue AS mature_rev, preclosure_revenue AS preclosure_rev,
                   sold_revenue AS sold_rev
            FROM propforce_sa.propforce_sa__sales_funnel
            WHERE user_id IN ({ids})
              AND (operation_type IS NULL OR operation_type != 'delete')
              AND entity_type = 'life_time'
              AND funnel_date::date = (
                  SELECT MAX(funnel_date::date)
                  FROM propforce_sa.propforce_sa__sales_funnel
                  WHERE user_id IN ({ids})
                    AND entity_type = 'life_time'
              )
        """,
        "auth": f"""
            SELECT user_id, success, COUNT(*) AS cnt
            FROM propforce_sa.propforce_sa__zn_pf_user_auth_activity
            WHERE user_id IN ({ids})
              AND created_at::date BETWEEN '{start}' AND '{end}'
            GROUP BY user_id, success
        """,
        "total_clients": f"""
            SELECT client_userid AS user_id, COUNT(DISTINCT clientid) AS cnt
            FROM propforce_sa.propforce_sa__zn_clients
            WHERE client_userid IN ({ids})
              AND (operation_type IS NULL OR operation_type != 'delete')
            GROUP BY client_userid
        """,
        "active_clients": f"""
            SELECT i.userid AS user_id, COUNT(DISTINCT i.clientid) AS cnt
            FROM propforce_sa.propforce_sa__zn_internal_inquiries i
            WHERE i.userid IN ({ids})
              AND (i.operation_type IS NULL OR i.operation_type != 'delete')
              AND i.status NOT IN (6, 8)
            GROUP BY i.userid
        """,
        "daily_tasks": f"""
            SELECT assigned_to AS user_id, date_added::date AS day, COUNT(*) AS cnt
            FROM propforce_sa.propforce_sa__zn_pf_task
            WHERE assigned_to IN ({ids})
              AND date_added::date BETWEEN '{start}' AND '{end}'
            GROUP BY assigned_to, day ORDER BY assigned_to, day
        """,
        "daily_leads": f"""
            SELECT userid AS user_id, time_added::date AS day, COUNT(DISTINCT inquiry_id) AS cnt
            FROM propforce_sa.propforce_sa__zn_internal_inquiries
            WHERE userid IN ({ids})
              AND (operation_type IS NULL OR operation_type != 'delete')
              AND time_added::date BETWEEN '{start}' AND '{end}'
            GROUP BY userid, day ORDER BY userid, day
        """,
        "qualified_calls": f"""
            SELECT user_id, COUNT(*) AS cnt
            FROM propforce_sa.propforce_sa__zn_pf_phone_calls
            WHERE user_id IN ({ids})
              AND (operation_type IS NULL OR operation_type != 'delete')
              AND duration_in_seconds >= 60
              AND date_added::date BETWEEN '{start}' AND '{end}'
            GROUP BY user_id
        """,
        "meetings_conducted": f"""
            SELECT t.assigned_to AS user_id, COUNT(*) AS cnt
            FROM propforce_sa.propforce_sa__zn_pf_task t
            JOIN propforce_sa.propforce_sa__zn_pf_task_types tt ON t.task_type_id = tt.task_type_id
            WHERE t.assigned_to IN ({ids})
              AND t.date_added::date BETWEEN '{start}' AND '{end}'
              AND (tt.key ILIKE '%meeting_done%'
                   OR tt.key IN ('outdoor','company_office','site_office',
                                 'google_meet','zoom_call','whatsapp_video_call','video_meeting',
                                 'walk_in'))
            GROUP BY t.assigned_to
        """,
        "leads_with_task_90d": f"""
            SELECT t.assigned_to AS user_id, COUNT(DISTINCT t.task_against_id) AS cnt
            FROM propforce_sa.propforce_sa__zn_pf_task t
            WHERE t.assigned_to IN ({ids}) AND t.task_against_id IS NOT NULL
              AND t.date_added >= DATEADD(day, -90, CURRENT_DATE)
            GROUP BY t.assigned_to
        """,
        "inactive_leads_30d": f"""
            SELECT i.userid AS user_id, i.inquiry_id, c.name AS client_name
            FROM propforce_sa.propforce_sa__zn_internal_inquiries i
            LEFT JOIN propforce_sa.propforce_sa__zn_clients c
                ON i.clientid = c.clientid AND (c.operation_type IS NULL OR c.operation_type != 'delete')
            LEFT JOIN (
                SELECT DISTINCT task_against_id
                FROM propforce_sa.propforce_sa__zn_pf_task
                WHERE assigned_to IN ({ids})
                  AND task_against_id IS NOT NULL
                  AND date_added >= DATEADD(day, -30, CURRENT_DATE)
            ) t ON i.inquiry_id = t.task_against_id
            WHERE i.userid IN ({ids})
              AND (i.operation_type IS NULL OR i.operation_type != 'delete')
              AND i.status NOT IN (6, 8)
              AND t.task_against_id IS NULL
        """,
        "pending_proposals": f"""
            WITH shared AS (
                SELECT t.assigned_to, t.task_against_id,
                       CASE WHEN tt.key LIKE 'is_%' THEN 'IS' ELSE 'IM' END AS ptype
                FROM propforce_sa.propforce_sa__zn_pf_task t
                JOIN propforce_sa.propforce_sa__zn_pf_task_types tt ON t.task_type_id = tt.task_type_id
                WHERE t.assigned_to IN ({ids})
                  AND t.date_added >= DATEADD(day, -30, CURRENT_DATE)
                  AND tt.key IN ('is_proposal_shared','im_proposal_shared')
                  AND t.task_against_id IS NOT NULL
            ),
            responded AS (
                SELECT t.assigned_to, t.task_against_id,
                       CASE WHEN tt.key LIKE 'is_%' THEN 'IS' ELSE 'IM' END AS ptype
                FROM propforce_sa.propforce_sa__zn_pf_task t
                JOIN propforce_sa.propforce_sa__zn_pf_task_types tt ON t.task_type_id = tt.task_type_id
                WHERE t.assigned_to IN ({ids})
                  AND tt.key IN ('is_proposal_accepted','is_proposal_rejected','im_proposal_accepted','im_proposal_rejected')
                  AND t.task_against_id IS NOT NULL
            )
            SELECT s.assigned_to AS user_id, s.ptype, COUNT(DISTINCT s.task_against_id) AS cnt
            FROM shared s
            LEFT JOIN responded r ON s.assigned_to = r.assigned_to AND s.task_against_id = r.task_against_id AND s.ptype = r.ptype
            WHERE r.task_against_id IS NULL
            GROUP BY s.assigned_to, s.ptype
        """,
        "qualified_clients": f"""
            SELECT t.assigned_to AS user_id, COUNT(DISTINCT t.client_id) AS cnt
            FROM propforce_sa.propforce_sa__zn_pf_task t
            JOIN propforce_sa.propforce_sa__zn_pf_task_types tt ON t.task_type_id = tt.task_type_id
            WHERE t.assigned_to IN ({ids})
              AND t.client_id IS NOT NULL
              AND tt.key = 'investment_brief_shared'
              AND t.date_added >= DATEADD(month, -3, CURRENT_DATE)
            GROUP BY t.assigned_to
        """,
        "pipeline_deals": f"""
            SELECT i.userid AS user_id, i.inquiry_id, i.clientid,
                   c.name AS client_name,
                   i.status, s.status_title, s.key AS status_key,
                   i.budget, i.time_added, i.time_updated
            FROM propforce_sa.propforce_sa__zn_internal_inquiries i
            LEFT JOIN propforce_sa.propforce_sa__zn_clients c
                ON i.clientid = c.clientid AND (c.operation_type IS NULL OR c.operation_type != 'delete')
            LEFT JOIN propforce_sa.propforce_sa__zn_internal_inquiries_status s
                ON i.status = s.status_id
            WHERE i.userid IN ({ids})
              AND (i.operation_type IS NULL OR i.operation_type != 'delete')
              AND i.status NOT IN (6, 8)
        """,
        "pipeline_notes": f"""
            SELECT m.organizer_id AS user_id, m.id AS meeting_id,
                   m.title, m.notes,
                   m.scheduled_start_time
            FROM propforce_sa.propforce_sa__management_meetings m
            JOIN propforce_sa.propforce_sa__management_meeting_types mt ON m.type_id = mt.id
            WHERE (m.operation_type IS NULL OR m.operation_type != 'delete')
              AND m.organizer_id IN ({ids})
              AND m.scheduled_start_time::date BETWEEN '{start}' AND '{end}'
            ORDER BY m.scheduled_start_time DESC
        """,
        "clients_added": f"""
            SELECT client_userid AS user_id, COUNT(DISTINCT clientid) AS cnt
            FROM propforce_sa.propforce_sa__zn_clients
            WHERE client_userid IN ({ids})
              AND (operation_type IS NULL OR operation_type != 'delete')
              AND time_added::date BETWEEN '{start}' AND '{end}'
            GROUP BY client_userid
        """,
        "proposals": f"""
            SELECT t.assigned_to AS user_id, tt.key AS task_key, COUNT(*) AS cnt
            FROM propforce_sa.propforce_sa__zn_pf_task t
            JOIN propforce_sa.propforce_sa__zn_pf_task_types tt ON t.task_type_id = tt.task_type_id
            WHERE t.assigned_to IN ({ids})
              AND t.date_added::date BETWEEN '{start}' AND '{end}'
              AND tt.key IN ('investment_brief_shared','investment_brief_accepted','investment_brief_rejected',
                             'is_proposal_shared','is_proposal_accepted','is_proposal_rejected',
                             'im_proposal_shared','im_proposal_accepted','im_proposal_rejected')
            GROUP BY t.assigned_to, tt.key
        """,
    }

    # Run queries — parallel locally, sequential on Railway (fewer connections)
    import os
    results = {}
    if os.environ.get("RAILWAY_ENVIRONMENT"):
        # Sequential on Railway to avoid connection pool issues
        conn = get_connection()
        for name, sql in sqls.items():
            try:
                results[name] = pd.read_sql(sql, conn)
            except Exception as e:
                print(f"Query error [{name}]: {e}")
                results[name] = pd.DataFrame()
        conn.close()
    else:
        # Parallel locally for speed
        with ThreadPoolExecutor(max_workers=12) as pool:
            futures = {pool.submit(q, sql): name for name, sql in sqls.items()}
            for future in as_completed(futures):
                try:
                    results[futures[future]] = future.result()
                except Exception as e:
                    print(f"Query error: {e}")
                    results[futures[future]] = pd.DataFrame()

    return results


def build_sparklines(dfs, name_map):
    """Build daily sparkline data for tasks and leads per WM with dates."""
    result = {}
    for key in ["daily_tasks", "daily_leads"]:
        df = dfs.get(key, pd.DataFrame())
        if df.empty:
            continue
        if "user_id" in df.columns:
            df["user_name"] = df["user_id"].map(name_map).fillna("Unknown")
        metric = key.replace("daily_", "")
        for name, grp in df.groupby("user_name"):
            if name not in result:
                result[name] = {}
            result[name][metric] = grp["cnt"].tolist()
            result[name][metric + "_dates"] = grp["day"].astype(str).tolist()
    return result


def build_response(dfs, wm_users, name_map):
    """Transform DataFrames into JSON-ready dicts."""
    # Add names
    for df in dfs.values():
        if not df.empty and "user_id" in df.columns:
            df["user_name"] = df["user_id"].map(name_map).fillna("Unknown")

    meetings = dfs["meetings"]
    attendees = dfs["attendees"]
    unique_mtg = dfs["unique_meetings"]
    tasks = dfs["tasks"]
    investors = dfs["investors"]
    leads = dfs["leads"]
    leads_worked_on = dfs["leads_worked_on"]
    pf_logins = dfs["pf_logins"]
    pf_logins_daily = dfs["pf_logins_daily"]
    pf_logins_bar = dfs["pf_logins_bar"]
    funnel = dfs["funnel"]
    auth = dfs["auth"]

    def umap(df, col="cnt"):
        return dict(zip(df["user_name"], df[col].astype(int))) if not df.empty else {}

    leads_map = umap(leads)
    worked_on_map = umap(leads_worked_on)
    investors_map = umap(investors)
    pf_map = umap(pf_logins)
    unique_mtg_map = umap(unique_mtg)

    # Attended maps
    att_map, morning_att_map = {}, {}
    if not attendees.empty:
        for un, g in attendees.groupby("user_name"):
            att_map[un] = int(g["cnt"].sum())
            mg = g[g["meeting_type"] == "morning_meeting"]
            morning_att_map[un] = int(mg["cnt"].sum()) if not mg.empty else 0

    morning_sched_map = {}
    if not meetings.empty:
        for un, g in meetings.groupby("user_name"):
            morning_sched_map[un] = int((g["meeting_type"] == "morning_meeting").sum())

    # ── OVERVIEW TABLE ──
    overview = []
    for uid, (name, _) in sorted(wm_users.items(), key=lambda x: x[1][0]):
        overview.append({
            "name": name,
            "leads_added": leads_map.get(name, 0),
            "leads_worked_on": worked_on_map.get(name, 0),
            "unique_meetings": unique_mtg_map.get(name, 0),
            "tasks_added": int(tasks[tasks["user_name"] == name]["cnt"].sum()) if not tasks.empty else 0,
            "pf_logins": pf_map.get(name, 0),
            "morning_scheduled": morning_sched_map.get(name, 0),
            "morning_attended": morning_att_map.get(name, 0),
        })
    overview.sort(key=lambda x: x["tasks_added"], reverse=True)

    # ── TASKS BREAKDOWN ──
    TASK_CATS = {
        "Google Meet": ["google_meet"],
        "Call": ["contact_client_call", "follow_up_call"],
        "Message": ["contact_client_message", "follow_up_message"],
        "Follow-Up": ["follow_up_call", "follow_up_message"],
        "WhatsApp": ["whatsapp_call", "whatsapp_message", "whatsapp_video_call"],
        "SMS": ["sms"],
        "Out-door Meeting": ["outdoor", "outdoor_meeting_arrange"],
        "Company Office": ["company_office", "company_office_meeting_arrange"],
    }
    task_cols = list(TASK_CATS.keys())
    task_breakdown = []
    for uid, (name, _) in wm_users.items():
        ug = tasks[tasks["user_id"] == uid] if not tasks.empty else pd.DataFrame()
        row = {"name": name, "investors_contacted": investors_map.get(name, 0),
               "total": int(ug["cnt"].sum()) if not ug.empty else 0}
        for cat, keys in TASK_CATS.items():
            cg = ug[ug["task_key"].isin(keys)] if not ug.empty else pd.DataFrame()
            row[cat] = int(cg["cnt"].sum()) if not cg.empty else 0
        task_breakdown.append(row)
    task_breakdown.sort(key=lambda x: x["total"], reverse=True)

    # ── PRE-SALES TABLE (IB / IS / IM) — only Shared, Accepted, Rejected ──
    IB_IS_IM = {
        "Investment Brief": {"shared": "investment_brief_shared",
                             "accepted": "investment_brief_accepted",
                             "rejected": "investment_brief_rejected"},
        "IS Proposal": {"shared": "is_proposal_shared",
                        "accepted": "is_proposal_accepted",
                        "rejected": "is_proposal_rejected"},
        "IM Proposal": {"shared": "im_proposal_shared",
                        "accepted": "im_proposal_accepted",
                        "rejected": "im_proposal_rejected"},
    }
    presales = []
    for uid, (name, _) in wm_users.items():
        ug = tasks[tasks["user_id"] == uid] if not tasks.empty else pd.DataFrame()
        row = {"name": name}
        for label, keys in IB_IS_IM.items():
            for sub, key_val in keys.items():
                col = f"{label} {sub.title()}"
                kg = ug[ug["task_key"] == key_val] if not ug.empty else pd.DataFrame()
                row[col] = int(kg["cnt"].sum()) if not kg.empty else 0
        presales.append(row)
    presales.sort(key=lambda x: sum(v for k, v in x.items() if k != "name"), reverse=True)
    presales_cols = []
    for label in IB_IS_IM:
        presales_cols += [f"{label} Shared", f"{label} Accepted", f"{label} Rejected"]

    # ── FUNNEL ──
    funnel_table = []
    if not funnel.empty:
        for _, row in funnel.iterrows():
            funnel_table.append({k: int(row[k]) if k != "user_name" else row[k]
                                 for k in ["user_name", "inquiries", "prospects", "mature",
                                            "preclosure", "sold", "closedlost",
                                            "mature_rev", "preclosure_rev", "sold_rev"]})
    fn_names = {r["user_name"] for r in funnel_table}
    for uid, (name, _) in wm_users.items():
        if name not in fn_names:
            funnel_table.append({"user_name": name, "inquiries": 0, "prospects": 0, "mature": 0,
                                 "preclosure": 0, "sold": 0, "closedlost": 0,
                                 "mature_rev": 0, "preclosure_rev": 0, "sold_rev": 0})
    funnel_table.sort(key=lambda x: x["inquiries"], reverse=True)

    # ── LOGIN CHARTS ──
    if not pf_logins_daily.empty:
        logins_trend = {"x": pf_logins_daily["day"].astype(str).tolist(),
                        "y": pf_logins_daily["cnt"].tolist()}
    else:
        logins_trend = {"x": [], "y": []}

    if not pf_logins_bar.empty:
        pf_logins_bar["user_name"] = pf_logins_bar["user_id"].map(name_map).fillna("Unknown")
        logins_bar = {"x": pf_logins_bar["user_name"].tolist(),
                      "y": pf_logins_bar["cnt"].tolist()}
    else:
        logins_bar = {"x": [], "y": []}

    # ── AUTH CHARTS ──
    if not auth.empty:
        asum = auth.groupby("success")["cnt"].sum().reset_index()
        asum["label"] = asum["success"].map({1: "Successful", 0: "Failed"})
        auth_donut = {"labels": asum["label"].tolist(), "values": [int(v) for v in asum["cnt"]]}
        ap = auth[auth["success"] == 1]
        if not ap.empty:
            ap = ap.sort_values("cnt", ascending=False)
            auth_bar = {"x": ap["user_name"].tolist(), "y": [int(v) for v in ap["cnt"]]}
        else:
            auth_bar = {"x": [], "y": []}
    else:
        auth_donut = {"labels": [], "values": []}
        auth_bar = {"x": [], "y": []}

    # ── KPI DASHBOARD TABLE ──
    total_clients_df = dfs["total_clients"]
    active_clients_df = dfs["active_clients"]
    qualified_calls_df = dfs["qualified_calls"]
    meetings_conducted_df = dfs["meetings_conducted"]
    clients_added_df = dfs["clients_added"]
    proposals_df = dfs["proposals"]

    for df in [total_clients_df, active_clients_df, qualified_calls_df, meetings_conducted_df, clients_added_df, proposals_df]:
        if not df.empty and "user_id" in df.columns:
            df["user_name"] = df["user_id"].map(name_map).fillna("Unknown")

    qualified_clients_df = dfs["qualified_clients"]
    if not qualified_clients_df.empty and "user_id" in qualified_clients_df.columns:
        qualified_clients_df["user_name"] = qualified_clients_df["user_id"].map(name_map).fillna("Unknown")
    qualified_map = dict(zip(qualified_clients_df["user_name"], qualified_clients_df["cnt"].astype(int))) if not qualified_clients_df.empty else {}

    total_clients_map = dict(zip(total_clients_df["user_name"], total_clients_df["cnt"].astype(int))) if not total_clients_df.empty else {}
    active_clients_map = dict(zip(active_clients_df["user_name"], active_clients_df["cnt"].astype(int))) if not active_clients_df.empty else {}
    clients_added_map = dict(zip(clients_added_df["user_name"], clients_added_df["cnt"].astype(int))) if not clients_added_df.empty else {}
    calls_map = dict(zip(qualified_calls_df["user_name"], qualified_calls_df["cnt"].astype(int))) if not qualified_calls_df.empty else {}
    mtg_conducted_map = dict(zip(meetings_conducted_df["user_name"], meetings_conducted_df["cnt"].astype(int))) if not meetings_conducted_df.empty else {}

    # Proposal maps
    def prop_count(key):
        if proposals_df.empty:
            return {}
        sub = proposals_df[proposals_df["task_key"] == key]
        return dict(zip(sub["user_name"], sub["cnt"].astype(int))) if not sub.empty else {}

    ib_shared_map = prop_count("investment_brief_shared")
    ib_accepted_map = prop_count("investment_brief_accepted")
    ib_rejected_map = prop_count("investment_brief_rejected")
    is_shared_map = prop_count("is_proposal_shared")
    is_accepted_map = prop_count("is_proposal_accepted")
    is_rejected_map = prop_count("is_proposal_rejected")
    im_shared_map = prop_count("im_proposal_shared")
    im_accepted_map = prop_count("im_proposal_accepted")
    im_rejected_map = prop_count("im_proposal_rejected")

    # Funnel maps from funnel_table
    funnel_map = {r["user_name"]: r for r in funnel_table}

    kpi_dashboard = []
    productivity_kpi = []
    conversion_kpi = []
    for uid, (name, _) in wm_users.items():
        tc = total_clients_map.get(name, 0)
        ac = active_clients_map.get(name, 0)
        active_pct = round(ac / tc * 100, 1) if tc > 0 else 0
        fn = funnel_map.get(name, {})
        qualified = qualified_map.get(name, 0)
        calls = calls_map.get(name, 0)
        meetings = mtg_conducted_map.get(name, 0)
        ca = clients_added_map.get(name, 0)
        ib_s = ib_shared_map.get(name, 0)
        ib_a = ib_accepted_map.get(name, 0)
        is_s = is_shared_map.get(name, 0)
        is_a = is_accepted_map.get(name, 0)
        im_s = im_shared_map.get(name, 0)
        im_a = im_accepted_map.get(name, 0)
        im_rate = round(im_a / im_s * 100, 1) if im_s > 0 else 0
        deals = fn.get("sold", 0)
        revenue = fn.get("sold_rev", 0)
        total_inquiries = fn.get("inquiries", 0)
        conv_is_close = round(deals / total_inquiries * 100, 1) if total_inquiries > 0 else 0
        conv_im_close = round(deals / im_s * 100, 1) if im_s > 0 else 0
        total_proposals = ib_s + is_s + im_s
        mtg_per_proposal = round(meetings / total_proposals, 1) if total_proposals > 0 else 0
        proposals_per_deal = round(total_proposals / deals, 1) if deals > 0 else 0
        calls_per_mtg = round(calls / meetings, 1) if meetings > 0 else 0

        kpi_dashboard.append({
            "name": name, "clients_added": ca, "active_client_pct": active_pct,
            "qualified_clients": qualified, "qualified_calls": calls,
            "meetings_conducted": meetings, "ib_shared": ib_s, "is_shared": is_s,
            "im_shared": im_s, "im_accepted": im_a, "im_acceptance_rate": im_rate,
            "deals_closed": deals, "revenue": revenue, "conv_is_close": conv_is_close,
            "conv_im_close": conv_im_close, "meetings_per_proposal": mtg_per_proposal,
            "proposals_per_deal": proposals_per_deal,
        })

        # Productivity KPI
        productivity_kpi.append({
            "name": name,
            "conv_is_close": conv_is_close,
            "conv_im_close": conv_im_close,
            "calls_per_meeting": calls_per_mtg,
            "meetings_per_proposal": mtg_per_proposal,
            "proposals_per_deal": proposals_per_deal,
        })

        # Pre-Sales Conversion
        ib_rate = round(ib_a / ib_s * 100, 1) if ib_s > 0 else 0
        is_rate = round(is_a / is_s * 100, 1) if is_s > 0 else 0
        ib_to_is = round(is_s / ib_a * 100, 1) if ib_a > 0 else 0
        is_to_im = round(im_s / is_a * 100, 1) if is_a > 0 else 0
        im_to_close = round(deals / im_a * 100, 1) if im_a > 0 else 0

        conversion_kpi.append({
            "name": name,
            "ib_shared": ib_s, "ib_accepted": ib_a, "ib_acceptance_rate": ib_rate,
            "is_shared": is_s, "is_accepted": is_a, "is_acceptance_rate": is_rate,
            "im_shared": im_s, "im_accepted": im_a, "im_acceptance_rate": im_rate,
            "ib_to_is_rate": ib_to_is,
            "is_to_im_rate": is_to_im,
            "im_to_closure_rate": im_to_close,
            "deals_closed": deals,
        })

    kpi_dashboard.sort(key=lambda x: x["qualified_calls"] + x["meetings_conducted"], reverse=True)
    productivity_kpi.sort(key=lambda x: x["calls_per_meeting"], reverse=True)
    conversion_kpi.sort(key=lambda x: x["ib_shared"] + x["is_shared"] + x["im_shared"], reverse=True)

    # ── AGEING ALERTS ──
    # Inactive leads = leads with no task in last 90 days
    leads_task_90d = dfs["leads_with_task_90d"]
    inactive_30d_df = dfs["inactive_leads_30d"]
    pending_props = dfs["pending_proposals"]

    if not leads_task_90d.empty and "user_id" in leads_task_90d.columns:
        leads_task_90d["user_name"] = leads_task_90d["user_id"].map(name_map).fillna("Unknown")
    if not inactive_30d_df.empty and "user_id" in inactive_30d_df.columns:
        inactive_30d_df["user_name"] = inactive_30d_df["user_id"].map(name_map).fillna("Unknown")
    if not pending_props.empty and "user_id" in pending_props.columns:
        pending_props["user_name"] = pending_props["user_id"].map(name_map).fillna("Unknown")

    active_90d_map = dict(zip(leads_task_90d["user_name"], leads_task_90d["cnt"].astype(int))) if not leads_task_90d.empty else {}

    # Total leads per WM (from overview leads_added lifetime or leads_map)
    total_leads_map = {}
    if not dfs["leads"].empty:
        for wm, grp in dfs["leads"].groupby("user_name") if "user_name" in dfs["leads"].columns else []:
            total_leads_map[wm] = len(grp["inquiry_id"].unique()) if "inquiry_id" in grp.columns else int(grp["cnt"].sum())

    # Use overview leads as total if available
    for r in overview:
        total_leads_map[r["name"]] = max(total_leads_map.get(r["name"], 0), r.get("leads_added", 0))

    inactive_leads_90d = []
    total_inactive = 0
    total_all = 0
    for uid, (name, _) in wm_users.items():
        tl = total_leads_map.get(name, 0)
        active = active_90d_map.get(name, 0)
        inactive = max(0, tl - active)
        pct = round(inactive / tl * 100, 1) if tl > 0 else 0
        inactive_leads_90d.append({"name": name, "total": tl, "active": active, "inactive": inactive, "pct": pct})
        total_inactive += inactive
        total_all += tl
    inactive_leads_90d.sort(key=lambda x: x["inactive"], reverse=True)

    # 30-day inactive leads with names
    inactive_30d_by_wm = {}
    if not inactive_30d_df.empty:
        for _, row in inactive_30d_df.iterrows():
            wm = row.get("user_name", "Unknown")
            cn = str(row.get("client_name") or "Unknown")
            if pd.isna(cn) or cn == "None":
                cn = "Unknown"
            if wm not in inactive_30d_by_wm:
                inactive_30d_by_wm[wm] = []
            inactive_30d_by_wm[wm].append(cn)

    inactive_30d_list = []
    total_30d = 0
    for uid, (name, _) in wm_users.items():
        leads_list = inactive_30d_by_wm.get(name, [])
        cnt = len(leads_list)
        inactive_30d_list.append({"name": name, "count": cnt, "leads": leads_list[:30]})
        total_30d += cnt
    inactive_30d_list.sort(key=lambda x: x["count"], reverse=True)

    # Pending proposals
    pending_is = {}
    pending_im = {}
    if not pending_props.empty:
        for _, row in pending_props.iterrows():
            n = row.get("user_name", "Unknown")
            if row["ptype"] == "IS":
                pending_is[n] = int(row["cnt"])
            else:
                pending_im[n] = int(row["cnt"])
    proposals_pending = []
    total_pending = 0
    for uid, (name, _) in wm_users.items():
        is_p = pending_is.get(name, 0)
        im_p = pending_im.get(name, 0)
        total_p = is_p + im_p
        proposals_pending.append({"name": name, "is_pending": is_p, "im_pending": im_p, "total": total_p})
        total_pending += total_p
    proposals_pending.sort(key=lambda x: x["total"], reverse=True)

    ageing_alerts = {
        "inactive_clients": inactive_leads_90d,
        "total_inactive": total_inactive,
        "total_clients": total_all,
        "inactive_pct": round(total_inactive / total_all * 100, 1) if total_all > 0 else 0,
        "inactive_30d": inactive_30d_list,
        "total_30d": total_30d,
        "proposals_pending": proposals_pending,
        "total_pending": total_pending,
    }

    # ── KPIs ──
    kpis = {
        "leads_added": sum(leads_map.values()),
        "leads_worked_on": sum(worked_on_map.values()),
        "unique_meetings": sum(unique_mtg_map.values()),
        "tasks_added": int(tasks["cnt"].sum()) if not tasks.empty else 0,
        "pf_logins": sum(pf_map.values()),
        "morning_scheduled": sum(morning_sched_map.values()),
        "morning_attended": sum(morning_att_map.values()),
        "auth_logins": int(auth[auth["success"] == 1]["cnt"].sum()) if not auth.empty else 0,
    }

    # ── PIPELINE ──
    pipeline_deals = dfs["pipeline_deals"]
    pipeline_notes = dfs["pipeline_notes"]
    if not pipeline_deals.empty and "user_id" in pipeline_deals.columns:
        pipeline_deals["user_name"] = pipeline_deals["user_id"].map(name_map).fillna("Unknown")

    # Stage mapping for pipeline view
    STAGE_MAP = {
        "new": "Early Engagement",
        "contacted_client_call": "Early Engagement",
        "contacted_client_message": "Early Engagement",
        "followed_up_call": "Early Engagement",
        "followed_up_message": "Early Engagement",
        "call_attempt": "Early Engagement",
        "contact_client_call_attempt": "Early Engagement",
        "sms": "Early Engagement",
        "whatsapp_call": "Early Engagement",
        "whatsapp_message": "Early Engagement",
        "google_meet": "Information Shared",
        "zoom_call": "Information Shared",
        "whatsapp_video_call": "Information Shared",
        "video_meeting": "Information Shared",
        "outdoor": "Due Diligence",
        "outdoor_meeting_arranged": "Due Diligence",
        "company_office": "Due Diligence",
        "company_office_meeting_arranged": "Due Diligence",
        "site_office": "Due Diligence",
        "walk_in": "Due Diligence",
        "meeting_done": "Due Diligence",
        "investment_brief": "Deal Making",
        "investment_brief_shared": "Deal Making",
        "is_proposal": "Deal Making",
        "is_proposal_shared": "Deal Making",
        "im_proposal": "Deal Making",
        "im_proposal_shared": "Deal Making",
        "token_payment": "Deal Making",
        "booking_form": "Deal Making",
        "sale_agreement": "Deal Making",
    }
    STAGE_ORDER = ["Early Engagement", "Information Shared", "Due Diligence", "Deal Making", "On Hold", "Paused"]
    STAGE_COLORS = {"Early Engagement": "#6366f1", "Information Shared": "#10b981",
                    "Due Diligence": "#f59e0b", "Deal Making": "#ef4444",
                    "On Hold": "#94a3b8", "Paused": "#64748b"}

    import re

    def generate_ai_summary(text):
        """Generate a 2-line summary by extracting key info from meeting notes."""
        if not text or len(text) < 50:
            return ""
        text_lower = text.lower()
        lines = []

        # Extract client/company names mentioned
        clients_mentioned = []
        for pat in [r'(?:meeting with|call with|spoke with|met with|contacted)\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)',
                    r'([A-Z][a-z]+(?:\s+[A-Z][a-z]+)+)\s*[-–]\s*(?:[A-Z])',]:
            for m in re.finditer(pat, text[:300]):
                name = m.group(1).strip()
                if len(name) > 4 and name not in clients_mentioned:
                    clients_mentioned.append(name)

        # Extract key outcomes
        outcomes = []
        if any(k in text_lower for k in ["meeting arranged", "meeting scheduled", "meeting confirmed"]):
            outcomes.append("meeting arranged")
        if any(k in text_lower for k in ["strong interest", "positive", "keen", "interested"]):
            outcomes.append("positive interest shown")
        if any(k in text_lower for k in ["follow up", "follow-up", "to review"]):
            outcomes.append("follow-up planned")
        if any(k in text_lower for k in ["shared info", "sent info", "circulate", "marketing material"]):
            outcomes.append("materials shared")
        if any(k in text_lower for k in ["not interested", "declined", "hold", "paused"]):
            outcomes.append("on hold / paused")
        if any(k in text_lower for k in ["investment", "allocat", "deploy", "capital"]):
            outcomes.append("investment discussion")
        if any(k in text_lower for k in ["hotel", "hospitality", "resort"]):
            outcomes.append("hospitality sector")
        if any(k in text_lower for k in ["real estate", "property", "development"]):
            outcomes.append("real estate focus")

        # Extract value mentions
        val_match = re.search(r'[\$]?\s*([\d,]+(?:\.\d+)?)\s*(?:m|million|M)', text)
        if val_match:
            outcomes.append(f"~${val_match.group(1)}M discussed")

        # Build summary
        if clients_mentioned:
            line1 = f"Engaged with {', '.join(clients_mentioned[:3])}"
            if len(clients_mentioned) > 3:
                line1 += f" +{len(clients_mentioned)-3} others"
            lines.append(line1)
        else:
            # Extract first meaningful sentence
            sentences = re.split(r'[.!?]\s+', text[:200])
            for s in sentences:
                s = s.strip()
                if len(s) > 20 and not s.lower().startswith(('daily', 'update', 'overview')):
                    lines.append(s[:100])
                    break

        if outcomes:
            lines.append("Key: " + " · ".join(outcomes[:4]))

        return " | ".join(lines[:2]) if lines else ""

    # Build notes index: for each WM, collect all notes text for client matching
    notes_by_wm = {}
    notes_list = []
    if not pipeline_notes.empty:
        if "user_id" in pipeline_notes.columns:
            pipeline_notes["user_name"] = pipeline_notes["user_id"].map(name_map).fillna("Unknown")
        for _, row in pipeline_notes.iterrows():
            notes_raw = str(row.get("notes") or "")
            notes_clean = re.sub(r'<[^>]+>', ' ', notes_raw).strip()
            notes_clean = re.sub(r'\s+', ' ', notes_clean)
            wm = row.get("user_name", "Unknown")
            if wm not in notes_by_wm:
                notes_by_wm[wm] = []
            notes_by_wm[wm].append(notes_clean)
            # Generate 2-line AI summary from note content
            ai_summary = generate_ai_summary(notes_clean)

            notes_list.append({
                "user_name": wm,
                "title": str(row.get("title") or ""),
                "notes": notes_clean[:500],
                "ai_summary": ai_summary,
                "date": str(row["scheduled_start_time"].date()) if pd.notna(row.get("scheduled_start_time")) else "",
            })

    def analyze_client_in_notes(client_name, wm_name, stage_from_status):
        """Analyze meeting notes to extract probability, stage, next steps, and deal value."""
        if not client_name or client_name == "Unknown Client":
            return stage_from_status, 10, "", 0

        cn_lower = client_name.lower()
        name_parts = client_name.split()
        mentions = []
        for note in notes_by_wm.get(wm_name, []):
            note_lower = note.lower()
            if cn_lower in note_lower or any(p.lower() in note_lower for p in name_parts if len(p) > 3):
                mentions.append(note)

        if not mentions:
            return stage_from_status, 10 if stage_from_status == "Early Engagement" else 15, "", 0

        combined = " ".join(mentions).lower()

        # ── Stage detection ──
        stage = stage_from_status
        if any(k in combined for k in ["on hold", "hold off", "paused", "not interested currently", "wait", "revisit later", "post-stabilization", "post stabilization"]):
            stage = "On Hold"
        elif any(k in combined for k in ["conflict", "hesitation", "not align", "outside", "not inclined", "currently focuss", "does not match", "too risky"]):
            stage = "Paused"
        elif any(k in combined for k in ["deal closed", "token", "booking form", "signed", "agreement signed", "proposal accepted", "committed to invest"]):
            stage = "Deal Making"
        elif any(k in combined for k in ["meeting arranged", "face-to-face", "in person", "in-person", "site visit", "due diligence", "review next", "met with", "meeting report", "took place"]):
            stage = "Due Diligence"
        elif any(k in combined for k in ["shared info", "sent info", "overview", "presented", "google meet", "zoom", "video call", "call report", "intro meeting", "introduction"]):
            stage = "Information Shared"

        # ── Probability scoring based on conversation quality ──
        prob = 10
        positive_signals = 0
        negative_signals = 0

        # Strong positive signals (high intent)
        for kw in ["committed", "ready to invest", "confirmed interest", "strong interest",
                    "keen", "wants to proceed", "agreed to", "next allocation",
                    "test scenario", "smaller initial allocation", "budget of",
                    "intends to utilize", "active investor", "decision-maker"]:
            if kw in combined:
                positive_signals += 3

        # Moderate positive signals (engaged)
        for kw in ["interest", "interested", "open to", "worth monitoring", "good prospect",
                    "positive", "productive", "strong contact", "strategic contact",
                    "high-value", "professional rapport", "continue the conversation",
                    "exploring", "wants to touch base", "will review", "acknowledged"]:
            if kw in combined:
                positive_signals += 1

        # Negative signals (disengaged)
        for kw in ["not interested", "declined", "rejected", "no exposure", "does not align",
                    "hesitation", "risk-conscious", "selective", "not currently",
                    "outside primary", "geopolitical", "conflict", "paused",
                    "focused on other markets", "not inclined"]:
            if kw in combined:
                negative_signals += 1

        # Meeting frequency boost
        if len(mentions) >= 3:
            positive_signals += 2
        elif len(mentions) >= 2:
            positive_signals += 1

        # Face-to-face meeting boost
        if any(k in combined for k in ["in person", "in-person", "face-to-face", "met with", "took place"]):
            positive_signals += 2

        # Score calculation
        net_score = positive_signals - negative_signals
        if stage == "Deal Making":
            prob = min(80, 55 + net_score * 3)
        elif stage == "Due Diligence":
            prob = min(50, 20 + net_score * 3)
        elif stage == "Information Shared":
            prob = min(35, 12 + net_score * 2)
        elif stage == "On Hold":
            prob = max(3, 8 - negative_signals)
        elif stage == "Paused":
            prob = max(2, 5 - negative_signals)
        else:  # Early Engagement
            prob = min(20, 8 + net_score * 2)
        prob = max(2, min(prob, 85))

        # ── Deal value extraction ──
        deal_value = 0
        value_patterns = [
            r'\$\s*([\d,]+(?:\.\d+)?)\s*(?:m|million)',
            r'([\d,]+(?:\.\d+)?)\s*(?:m|million)\s*(?:usd|dollar|\$)',
            r'sar\s*([\d,]+(?:\.\d+)?)\s*(?:m|million)',
            r'budget\s*(?:of\s*)?(?:approximately\s*)?(?:sar\s*)?([\d,]+(?:\.\d+)?)\s*(?:m|million)',
            r'([\d,]+(?:\.\d+)?)\s*(?:m|million)\s*(?:budget|revenue|value|pipeline)',
            r'\$\s*([\d,]+)\s*(?:k|thousand)',
            r'sar\s*([\d,]+(?:,\d{3})*)',
            r'budget\s*(?:of\s*)?(?:approximately\s*)?(?:sar\s*)?([\d,]+(?:,\d{3})*)',
        ]
        for pat in value_patterns:
            match = re.search(pat, combined)
            if match:
                val_str = match.group(1).replace(',', '')
                try:
                    val = float(val_str)
                    if 'million' in combined[max(0, match.start()-5):match.end()+15].lower() or 'm' in combined[match.end():match.end()+3].lower():
                        val *= 1000000
                    elif 'thousand' in combined[max(0, match.start()-5):match.end()+15].lower() or 'k' in combined[match.end():match.end()+3].lower():
                        val *= 1000
                    elif val < 100:
                        val *= 1000000  # assume millions if small number
                    deal_value = int(val)
                except:
                    pass
                if deal_value > 0:
                    break

        # ── Next steps extraction ──
        next_step = ""
        for note in mentions:
            note_lower = note.lower()
            patterns = [
                r'next\s*step[s]?\s*[:\.]\s*([^\.]{10,120})',
                r'follow[\s-]*up\s+(?:on|with|re|to)\s+([^\.]{10,100})',
                r'meeting\s+(?:arranged|scheduled|confirmed)\s+([^\.]{5,80})',
                r'arranging\s+(?:call|meeting)\s+([^\.]{5,80})',
                r'to\s+(?:review|arrange|discuss|share|send|contact|confirm|re-engage|present)\s+([^\.]{5,80})',
                r'will\s+(?:arrange|share|send|circulate|contact|follow|maintain|prepare)\s+([^\.]{5,80})',
                r'agreement\s+(?:reached|to)\s+([^\.]{5,80})',
            ]
            for pat in patterns:
                match = re.search(pat, note_lower)
                if match:
                    next_step = match.group(0).strip()[:120]
                    next_step = next_step[0].upper() + next_step[1:] if next_step else ""
                    break
            if next_step:
                break

        return stage, prob, next_step, deal_value

    # Build deals list with analysis
    deals_list = []
    if not pipeline_deals.empty:
        for _, row in pipeline_deals.iterrows():
            sk = row.get("status_key") or "new"
            base_stage = STAGE_MAP.get(sk, "Early Engagement")
            cn = row.get("client_name") or "Unknown Client"
            cn_str = str(cn) if pd.notna(cn) else "Unknown Client"
            wm = row["user_name"]

            stage, prob, next_step, est_value = analyze_client_in_notes(cn_str, wm, base_stage)

            # Deal age in days
            from datetime import date as date_type
            added_date = row["time_added"].date() if pd.notna(row.get("time_added")) else None
            updated_date = row["time_updated"].date() if pd.notna(row.get("time_updated")) else None
            today = date_type.today()
            deal_age = (today - added_date).days if added_date else 0
            days_since_update = (today - updated_date).days if updated_date else deal_age
            budget_val = int(row["budget"]) if pd.notna(row.get("budget")) and row.get("budget") else 0

            final_value = est_value if est_value > 0 else budget_val
            deals_list.append({
                "user_name": wm,
                "client_name": cn_str,
                "stage": stage,
                "status_title": str(row.get("status_title") or "New"),
                "probability": prob,
                "next_step": next_step,
                "budget": budget_val,
                "est_deal_value": est_value,
                "weighted_value": int(final_value * prob / 100),
                "deal_age": deal_age,
                "days_since_update": days_since_update,
                "added": str(added_date) if added_date else "",
                "updated": str(updated_date) if updated_date else "",
            })

    # Stage counts per WM
    stage_counts = {}
    for d in deals_list:
        n = d["user_name"]
        s = d["stage"]
        if n not in stage_counts:
            stage_counts[n] = {st: 0 for st in STAGE_ORDER}
        if s in stage_counts[n]:
            stage_counts[n][s] += 1

    # ── STRATEGIC INSIGHTS (for Insights tab) ──

    # 1. Weighted Pipeline per WM
    weighted_by_wm = {}
    deals_by_stage = {"Early Engagement": 0, "Information Shared": 0, "Due Diligence": 0,
                      "Deal Making": 0, "On Hold": 0, "Paused": 0}
    new_deals_period = 0
    moved_forward = 0
    for d in deals_list:
        wm = d["user_name"]
        weighted_by_wm[wm] = weighted_by_wm.get(wm, 0) + d["weighted_value"]
        if d["stage"] in deals_by_stage:
            deals_by_stage[d["stage"]] += 1
        if d["deal_age"] <= 30:
            new_deals_period += 1
        if d["stage"] in ("Due Diligence", "Deal Making") and d["days_since_update"] <= 14:
            moved_forward += 1

    total_weighted = sum(weighted_by_wm.values())
    total_gross = sum(d["budget"] for d in deals_list)

    # 2. Client engagement — last interaction per client per WM
    client_engagement = []
    if not pipeline_deals.empty:
        ce = pipeline_deals.groupby(["user_name", "client_name"]).agg(
            last_update=("time_updated", "max")
        ).reset_index()
        ce = ce.sort_values("last_update", ascending=False)
        from datetime import date as dt_date
        for _, row in ce.head(50).iterrows():
            lu = row["last_update"]
            days_ago = (dt_date.today() - lu.date()).days if pd.notna(lu) else 999
            bucket = "< 7d" if days_ago < 7 else "7-14d" if days_ago < 14 else "14-30d" if days_ago < 30 else "30-60d" if days_ago < 60 else "60d+"
            client_engagement.append({
                "user_name": row["user_name"],
                "client_name": str(row["client_name"]) if pd.notna(row["client_name"]) else "Unknown",
                "days_ago": days_ago,
                "bucket": bucket,
                "last_date": str(lu.date()) if pd.notna(lu) else "",
            })

    # Client engagement heatmap data
    engagement_heatmap = {}
    for ce_row in client_engagement:
        wm = ce_row["user_name"]
        b = ce_row["bucket"]
        if wm not in engagement_heatmap:
            engagement_heatmap[wm] = {"< 7d": 0, "7-14d": 0, "14-30d": 0, "30-60d": 0, "60d+": 0}
        engagement_heatmap[wm][b] += 1

    # 3. Market themes from meeting notes
    all_notes_text = " ".join([n["notes"] for n in notes_list]).lower()
    theme_keywords = {
        "Hospitality / Hotels": ["hotel", "hospitality", "resort", "accommodation"],
        "Family Office": ["family office", "multi family", "single family office"],
        "Real Estate / Development": ["real estate", "development", "property", "residential", "commercial"],
        "Institutional / PE": ["institutional", "private equity", "pe fund", "fund manager"],
        "HNWI / UHNWI": ["hnwi", "uhnwi", "high net worth", "ultra high"],
        "Saudi / KSA": ["saudi", "ksa", "kingdom", "riyadh", "jeddah"],
        "Dubai / UAE": ["dubai", "uae", "abu dhabi", "emirates"],
        "Logistics / Industrial": ["logistics", "industrial", "warehouse"],
        "Retail / Mixed Use": ["retail", "mixed use", "commercial"],
        "Banking / Finance": ["bank", "banker", "finance", "capital"],
    }
    market_themes = []
    for theme, keywords in theme_keywords.items():
        count = sum(all_notes_text.count(kw) for kw in keywords)
        if count > 0:
            market_themes.append({"theme": theme, "mentions": count})
    market_themes.sort(key=lambda x: x["mentions"], reverse=True)

    # 4. Deal velocity — average days in each stage
    stage_velocity = {}
    for s in STAGE_ORDER:
        stage_deals = [d for d in deals_list if d["stage"] == s]
        if stage_deals:
            avg_age = round(sum(d["deal_age"] for d in stage_deals) / len(stage_deals))
            stage_velocity[s] = {"count": len(stage_deals), "avg_days": avg_age}

    # 5. Team pipeline health
    pipeline_health = {
        "total_deals": len(deals_list),
        "total_gross": total_gross,
        "total_weighted": total_weighted,
        "dd_plus": deals_by_stage.get("Due Diligence", 0) + deals_by_stage.get("Deal Making", 0),
        "new_deals_period": new_deals_period,
        "moved_forward": moved_forward,
        "on_hold": deals_by_stage.get("On Hold", 0),
        "paused": deals_by_stage.get("Paused", 0),
        "weighted_by_wm": weighted_by_wm,
    }

    strategic_insights = {
        "pipeline_health": pipeline_health,
        "stage_velocity": stage_velocity,
        "engagement_heatmap": engagement_heatmap,
        "market_themes": market_themes,
        "deals_by_stage": deals_by_stage,
    }

    pipeline_data = {
        "deals": deals_list,
        "total_deals": len(deals_list),
        "stage_counts": stage_counts,
        "stage_order": STAGE_ORDER,
        "stage_colors": STAGE_COLORS,
        "notes": notes_list[:50],
    }

    return {
        "kpis": kpis,
        "overview": overview,
        "kpi_dashboard": kpi_dashboard,
        "productivity_kpi": productivity_kpi,
        "ageing_alerts": ageing_alerts,
        "conversion_kpi": conversion_kpi,
        "sparklines": build_sparklines(dfs, name_map),
        "pipeline": pipeline_data,
        "insights": strategic_insights,
        "task_breakdown": task_breakdown,
        "task_cols": task_cols,
        "presales": presales,
        "presales_cols": presales_cols,
        "funnel_table": funnel_table,
        "logins_trend": logins_trend,
        "logins_bar": logins_bar,
        "auth_donut": auth_donut,
        "auth_bar": auth_bar,
    }


# ── Routes ───────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    start, end = default_dates()
    return render_template("dashboard.html", start_date=start, end_date=end)


@app.route("/api/wm_list")
def api_wm_list():
    return jsonify([
        {"id": uid, "name": n, "designation": d}
        for uid, (n, d) in sorted(WM_USERS.items(), key=lambda x: x[1][0])
    ])


@app.route("/api/data")
def api_data():
    start = request.args.get("start", default_dates()[0])
    end = request.args.get("end", default_dates()[1])
    wm_filter = request.args.get("wm", "")

    # Check cache
    cache_key = f"{start}|{end}|{wm_filter}"
    cached = get_cached(cache_key)
    if cached:
        return jsonify(cached)

    wm_users = WM_USERS.copy()
    name_map = NAME_MAP.copy()

    if wm_filter:
        selected = set(int(x) for x in wm_filter.split(",") if x.strip())
        wm_users = {uid: info for uid, info in wm_users.items() if uid in selected}
        name_map = {uid: name for uid, name in name_map.items() if uid in selected}

    ids = ",".join(str(uid) for uid in wm_users)
    if not ids:
        return jsonify({"error": "No WM users selected"})

    try:
        dfs = fetch_all(ids, start, end)
        result = build_response(dfs, wm_users, name_map)
        set_cached(cache_key, result)
        return jsonify(result)
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    import os
    port = int(os.environ.get("PORT", 5000))
    debug = os.environ.get("RAILWAY_ENVIRONMENT") is None  # debug only locally
    app.run(debug=debug, host="0.0.0.0", port=port)
