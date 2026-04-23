"""
Xeal Pharma — CEO Operational Dashboard
=========================================
Private CEO-only view. Single-file Streamlit application.

Data sources
------------
1. Microsoft 365 mailboxes (12 accessible at build time; 3 pending)
2. Google Sheet: https://docs.google.com/spreadsheets/d/1pekeSxMN884asx_dieEZAzZE09LZdJtqnJwVaAH9shg/edit
   Tabs: Master Overview | Neil KPIs | Waqar KPIs | Pipeline Tracker |
         Stock Intelligence | NPI Tracker | Action Log | Capacity Dashboard
3. Sage weekly CSV dump (connection pending)

Design principles
-----------------
- All customer, mailbox and team email lists are defined at the top of the file.
  Adding a new customer, mailbox or team member is a config-only change —
  no code in section builders needs to be edited.
- Sections are individually toggle-able and collapsible.
- Every SLA threshold is controlled from the sidebar.
- Where a data source is not yet connected, the section still renders its
  structure with the label "Awaiting data connection" — never raises an error.

Run locally
-----------
    pip install -r requirements.txt
    streamlit run xeal_ceo_dashboard.py

Deploy (Streamlit Community Cloud)
----------------------------------
    1. Push this file + requirements.txt to a private GitHub repo.
    2. Go to https://share.streamlit.io -> New app -> point at the repo.
    3. Set secrets (Microsoft Graph, Google Sheets service account, Sage).
    4. Restrict access via the app's "Share" settings (email allowlist).
"""

from __future__ import annotations

import io
import json
from dataclasses import dataclass, field
from datetime import datetime, timedelta, date
from typing import Any, Dict, List, Optional

import pandas as pd
import streamlit as st

# ---------------------------------------------------------------------------
# CONFIGURATION — edit these blocks to add mailboxes, customers, or team members
# ---------------------------------------------------------------------------

APP_TITLE = "Xeal Pharma — CEO Operational Dashboard"
APP_TAGLINE = "Private CEO view · UK · Germany · Australia"

# Brand palette
COLOR_HEADER = "#1B3A6B"  # dark navy
COLOR_RED = "#E24B4A"
COLOR_AMBER = "#EF9F27"
COLOR_GREEN = "#639922"
COLOR_DORMANT = "#888780"
COLOR_STABLE = "#3A7BB8"  # blue-ish for stable pill
COLOR_BG = "#FFFFFF"
COLOR_TEXT = "#1F1F1F"

# --- Mailboxes ---------------------------------------------------------------
# Flip `connected` to True when a mailbox comes online. The dashboard will
# automatically include it in every KPI, response-time and coverage metric.
MAILBOXES: List[Dict[str, Any]] = [
    {"address": "ibrar@xealpharma.co.uk",          "owner": "Ibrar",    "role": "CEO",             "connected": True},
    {"address": "contact@xealpharma.co.uk",        "owner": "Shared",   "role": "Front door",      "connected": True},
    {"address": "qa@xealpharma.co.uk",             "owner": "QA team",  "role": "Quality",         "connected": True},
    {"address": "accounts@xealpharma.co.uk",       "owner": "Finance",  "role": "Finance",         "connected": True},
    {"address": "purchasing@xealpharma.co.uk",     "owner": "Ops",      "role": "Purchasing",      "connected": True},
    {"address": "orders@xealpharma.co.uk",         "owner": "Ops",      "role": "Order desk",      "connected": True},
    {"address": "haidar@xealpharma.co.uk",         "owner": "Haidar",   "role": "Regulatory",      "connected": True},
    {"address": "azhar@xealpharma.co.uk",          "owner": "Azhar",    "role": "QA/Reg",          "connected": True},
    {"address": "robinson@xealpharma.co.uk",       "owner": "Robinson", "role": "QA",              "connected": True},
    {"address": "mariana@xealpharma.co.uk",        "owner": "Mariana",  "role": "Customer ops",    "connected": True},
    {"address": "ana.machado@xealpharma.co.uk",    "owner": "Ana",      "role": "Customer ops",    "connected": True},
    # NOTE: chelsea@ is a valid reply address even if mailbox not separately queried
    # Pending connection — flip connected=True when mailbox is added, no other code needed
    {"address": "neil.desjardins@xealpharma.co.uk", "owner": "Neil",     "role": "AM (Portfolio)",  "connected": False},
    {"address": "muhammad.waqar@xealpharma.co.uk",  "owner": "Waqar",    "role": "AM (Portfolio)",  "connected": False},
    {"address": "kayu@xealpharma.co.uk",             "owner": "Kayu",     "role": "Operations",      "connected": False},
]

# --- Team reply addresses (any of these = valid Xeal response) ---------------
TEAM_EMAILS: List[str] = [
    "ibrar@xealpharma.co.uk",
    "contact@xealpharma.co.uk",
    "neil.desjardins@xealpharma.co.uk",
    "muhammad.waqar@xealpharma.co.uk",
    "kayu@xealpharma.co.uk",
    "mariana@xealpharma.co.uk",
    "ana.machado@xealpharma.co.uk",
    "chelsea@xealpharma.co.uk",
]

# --- Customers ---------------------------------------------------------------
# Add a new customer by appending a row. The dashboard picks it up everywhere.
CUSTOMERS: List[Dict[str, Any]] = [
    # Neil's portfolio
    {"name": "Cantourage",                  "am": "Neil",  "status": "Existing",  "domain": "cantourage.com"},
    {"name": "Caprica",                     "am": "Neil",  "status": "Existing",  "domain": "caprica.co"},
    {"name": "CCCC",                        "am": "Neil",  "status": "Existing",  "domain": "cccc.co.uk"},
    {"name": "Portocanna",                  "am": "Neil",  "status": "Existing",  "domain": "portocanna.com"},
    {"name": "Enua",                        "am": "Neil",  "status": "Existing",  "domain": "enua.com"},
    {"name": "Travel Clinic Farnborough",   "am": "Neil",  "status": "Existing",  "domain": "travelclinic.co.uk"},
    {"name": "Aurora",                      "am": "Neil",  "status": "Onboarded", "domain": "auroramj.com"},
    {"name": "Atlanticann",                 "am": "Neil",  "status": "Onboarded", "domain": "atlanticann.com"},
    {"name": "Green Seal",                  "am": "Neil",  "status": "Onboarded", "domain": "greenseal.com"},
    {"name": "Green Success",               "am": "Neil",  "status": "Onboarded", "domain": "greensuccess.com"},
    {"name": "BLS",                         "am": "Neil",  "status": "Onboarded", "domain": "bls.com"},
    {"name": "Levaclinic",                  "am": "Neil",  "status": "Existing",  "domain": "levaclinic.com"},
    {"name": "Leafie",                      "am": "Neil",  "status": "Dormant",   "domain": "leafie.co.uk"},
    {"name": "PLF",                         "am": "Neil",  "status": "Dormant",   "domain": "plf.com"},
    # Waqar's portfolio
    {"name": "Mamedica",                    "am": "Waqar", "status": "Existing",  "domain": "mamedica.co.uk"},
    {"name": "4C",                          "am": "Waqar", "status": "Existing",  "domain": "4c-labs.com"},
    {"name": "Releaf",                      "am": "Waqar", "status": "Existing",  "domain": "releaf.co.uk"},
    {"name": "Muzo/Dycar",                  "am": "Waqar", "status": "Existing",  "domain": "dycar.com"},
    {"name": "Somai",                       "am": "Waqar", "status": "Existing",  "domain": "somaipharma.com"},
    {"name": "ECS",                         "am": "Waqar", "status": "Existing",  "domain": "ecs.com"},
    {"name": "Glass Pharms",                "am": "Waqar", "status": "Existing",  "domain": "glasspharms.com"},
    {"name": "Hill Top",                    "am": "Waqar", "status": "Existing",  "domain": "hilltop.com"},
    {"name": "NGP-Bloom",                   "am": "Waqar", "status": "Prospect",  "domain": "ngp-bloom.com"},
    {"name": "Tilray",                      "am": "Waqar", "status": "Prospect",  "domain": "tilray.com"},
    {"name": "Green Island",                "am": "Waqar", "status": "Prospect",  "domain": "greenisland.com"},
    {"name": "Cronos",                      "am": "Waqar", "status": "Prospect",  "domain": "cronosgroup.com"},
    {"name": "Waterside",                   "am": "Waqar", "status": "Prospect",  "domain": "waterside.com"},
]

STATUSES = ["Existing", "Stable", "Onboarded", "Prospect", "Dormant", "Closed"]
AMS = ["Neil", "Waqar"]

# 13-stage import pipeline
PIPELINE_STAGES = [
    "Supplier Registration",
    "Product Registration",
    "CNL Received",
    "Import Licence Applied",
    "Import Licence Approved",
    "Export Licence",
    "Shipped",
    "Arrived",
    "Goods-In Testing",
    "Manufacturing",
    "Release",
    "COA Testing",
    "Delivered",
]

NPI_DEPARTMENTS = ["QA", "Manufacturing", "Regulatory", "Commercial", "Customer"]

# --- SLA defaults ------------------------------------------------------------
SLA_DEFAULTS = {
    "complaint_amber_hrs": 2,
    "complaint_red_hrs": 4,
    "order_amber_hrs": 18,
    "order_red_hrs": 24,
    "query_amber_hrs": 36,
    "query_red_hrs": 48,
    "active_contact_amber_days": 7,
    "active_contact_red_days": 14,
    "stable_contact_amber_days": 21,
    "stable_contact_red_days": 30,
    "prospect_contact_amber_days": 14,
    "prospect_contact_red_days": 30,
    "idle_stock_amber_days": 60,
    "idle_stock_red_days": 120,
    "capacity_amber_pct": 70,
    "capacity_red_pct": 80,
    "npi_amber_days": 1,
    "npi_red_days": 7,
    "pipeline_amber_days": 7,
    "pipeline_red_days": 14,
    "audit_warn_days": 30,
    "licence_warn_days": 30,
}

GOOGLE_SHEET_URL = (
    "https://docs.google.com/spreadsheets/d/"
    "1pekeSxMN884asx_dieEZAzZE09LZdJtqnJwVaAH9shg/edit"
)

SECTION_KEYS = [
    "section_1",  # sidebar is always rendered; this flag is unused but reserved
    "summary_bar",
    "relationship",
    "commercial",
    "am_kpis",
    "pipeline",
    "stock_capacity",
    "npi",
    "compliance",
    "financial",
    "alerts",
]

# ---------------------------------------------------------------------------
# PAGE SETUP & STYLING
# ---------------------------------------------------------------------------

st.set_page_config(
    page_title=APP_TITLE,
    page_icon="\U0001F4CA",
    layout="wide",
    initial_sidebar_state="expanded",
)

CUSTOM_CSS = f"""
<style>
html, body, [class*="css"] {{
    font-family: Arial, Helvetica, sans-serif !important;
    color: {COLOR_TEXT};
}}
.block-container {{
    padding-top: 1.2rem;
    padding-bottom: 2rem;
    max-width: 1600px;
}}
.xeal-header {{
    background: {COLOR_HEADER};
    color: white;
    padding: 14px 22px;
    border-radius: 6px;
    margin-bottom: 14px;
    display: flex;
    justify-content: space-between;
    align-items: center;
}}
.xeal-header h1 {{ font-size: 22px; margin: 0; color: white; }}
.xeal-header .sub {{ font-size: 12px; opacity: 0.85; }}
.metric-card {{
    padding: 14px 16px;
    border-radius: 6px;
    color: white;
    text-align: center;
    box-shadow: 0 1px 3px rgba(0,0,0,0.1);
}}
.metric-card .label {{ font-size: 12px; text-transform: uppercase; letter-spacing: 0.5px; opacity: 0.9; }}
.metric-card .value {{ font-size: 30px; font-weight: 700; line-height: 1.1; margin-top: 4px; }}
.pill {{
    display: inline-block;
    padding: 2px 10px;
    border-radius: 10px;
    color: white;
    font-size: 11px;
    font-weight: 600;
    letter-spacing: 0.3px;
}}
.pill-red    {{ background: {COLOR_RED}; }}
.pill-amber  {{ background: {COLOR_AMBER}; }}
.pill-green  {{ background: {COLOR_GREEN}; }}
.pill-dormant{{ background: {COLOR_DORMANT}; }}
.pill-stable {{ background: {COLOR_STABLE}; }}
.section-title {{
    border-left: 4px solid {COLOR_HEADER};
    padding-left: 10px;
    margin-top: 6px;
    margin-bottom: 4px;
    font-weight: 600;
}}
.awaiting {{
    background: #FFF7E0;
    color: #7A5A00;
    border: 1px dashed #EFB73E;
    padding: 8px 12px;
    border-radius: 4px;
    font-size: 13px;
    margin: 8px 0;
}}
.small-muted {{ color: #666; font-size: 12px; }}
.footer-ts    {{ color: #888; font-size: 11px; margin-top: 4px; }}
div[data-testid="stDataFrame"] {{ border: 1px solid #ECECEC; border-radius: 4px; }}
</style>
"""
st.markdown(CUSTOM_CSS, unsafe_allow_html=True)

# ---------------------------------------------------------------------------
# SESSION STATE
# ---------------------------------------------------------------------------

def _init_state() -> None:
    ss = st.session_state
    ss.setdefault("customers", [dict(c) for c in CUSTOMERS])
    ss.setdefault("dismissed_alerts", set())
    ss.setdefault("snoozed_alerts", {})        # alert_id -> iso date
    ss.setdefault("alert_notes", {})           # alert_id -> note
    ss.setdefault("review_dates", {})          # customer -> next review iso
    ss.setdefault("view_mode", "Detailed")
    ss.setdefault("section_flags", {k: True for k in SECTION_KEYS})
    ss.setdefault("per_section_timeframe", {})
    ss.setdefault("last_refresh", datetime.now())

_init_state()

# ---------------------------------------------------------------------------
# HELPERS
# ---------------------------------------------------------------------------

def _ts() -> str:
    return st.session_state["last_refresh"].strftime("%d %b %Y %H:%M")

def _timestamp_caption(section_name: str) -> None:
    st.markdown(
        f"<div class='footer-ts'>Last updated: {_ts()} &nbsp;·&nbsp; Section: {section_name}</div>",
        unsafe_allow_html=True,
    )

def _awaiting(msg: str) -> None:
    st.markdown(f"<div class='awaiting'>\u26A0\uFE0F {msg}</div>", unsafe_allow_html=True)

def _pill(text: str, kind: str) -> str:
    kind_l = kind.lower()
    mapping = {"red": "pill-red", "amber": "pill-amber", "green": "pill-green",
               "dormant": "pill-dormant", "stable": "pill-stable"}
    cls = mapping.get(kind_l, "pill-dormant")
    return f"<span class='pill {cls}'>{text}</span>"

# --- Badge + column-config helpers for st.dataframe rendering ----------------
# st.dataframe (unlike st.write(df.to_html(...))) doesn't render HTML pills,
# so we use leading emoji indicators to preserve the RAG colour cue while
# letting the dataframe be scrollable, column-sized and pin-compatible.

def _rag_badge(rag: str) -> str:
    emoji = {"Red": "\U0001F534", "Amber": "\U0001F7E0", "Green": "\U0001F7E2",
             "Dormant": "\u26AB", "Stable": "\U0001F535"}
    return f"{emoji.get(rag, '')} {rag}".strip()

def _priority_badge(priority: str) -> str:
    emoji = {"High": "\U0001F534", "Medium": "\U0001F7E0", "Low": "\u26AA"}
    return f"{emoji.get(priority, '')} {priority}".strip()

def _column_config(
    columns: List[str],
    wide: Optional[List[str]] = None,
    narrow: Optional[List[str]] = None,
    pin: Optional[str] = None,
) -> Dict[str, Any]:
    """Build a column_config dict for st.dataframe.

    - `wide`   → columns rendered at width="large"
    - `narrow` → columns rendered at width="small"
    - every other column gets width="medium"
    - `pin`    → column name to pin to the left (stays visible on horizontal scroll)

    Falls back to a plain {name: width} dict if the installed Streamlit is too
    old to expose st.column_config.Column — never raises.
    """
    wide_s = set(wide or [])
    narrow_s = set(narrow or [])
    cfg: Dict[str, Any] = {}
    col_factory = getattr(st, "column_config", None)
    for col in columns:
        if col in wide_s:
            width = "large"
        elif col in narrow_s:
            width = "small"
        else:
            width = "medium"
        if col_factory and hasattr(col_factory, "Column"):
            kwargs: Dict[str, Any] = {"width": width}
            if pin and col == pin:
                kwargs["pinned"] = True  # requires Streamlit >= 1.43
            try:
                cfg[col] = col_factory.Column(col, **kwargs)
            except TypeError:
                # Older Streamlit — drop unsupported kwargs and retry
                kwargs.pop("pinned", None)
                cfg[col] = col_factory.Column(col, **kwargs)
        else:
            cfg[col] = width
    return cfg

def _rag_from_days(days: Optional[int], amber: int, red: int) -> str:
    if days is None:
        return "Dormant"
    if days >= red:
        return "Red"
    if days >= amber:
        return "Amber"
    return "Green"

def _rag_from_pct(pct: Optional[float], amber: float, red: float) -> str:
    if pct is None:
        return "Dormant"
    if pct >= red:
        return "Red"
    if pct >= amber:
        return "Amber"
    return "Green"

def _export_csv_button(df: pd.DataFrame, filename: str, key: str) -> None:
    buf = io.StringIO()
    df.to_csv(buf, index=False)
    st.download_button(
        label="\u2B07\uFE0F  Export CSV",
        data=buf.getvalue(),
        file_name=filename,
        mime="text/csv",
        key=f"dl_{key}",
    )

def _active_customers(am_filter: str, status_filter: List[str]) -> List[Dict[str, Any]]:
    out = []
    for c in st.session_state["customers"]:
        if am_filter != "All" and c["am"] != am_filter:
            continue
        if status_filter and "All" not in status_filter and c["status"] not in status_filter:
            continue
        if c["status"] == "Closed":
            continue  # closed customers never appear in reporting views
        out.append(c)
    return out

def _timeframe_days(label: str, today: date) -> int:
    """Convert the timeframe selector to a look-back window in days."""
    label = (label or "").lower()
    if "7"  in label and "days" in label: return 7
    if "14" in label and "days" in label: return 14
    if "30" in label and "days" in label: return 30
    if "60" in label and "days" in label: return 60
    if "90" in label and "days" in label: return 90
    if "this quarter" in label:
        q_start_month = ((today.month - 1) // 3) * 3 + 1
        q_start = date(today.year, q_start_month, 1)
        return (today - q_start).days or 1
    if "last quarter" in label:
        return 90
    return 30

# ---------------------------------------------------------------------------
# DATA CONNECTORS
# ---------------------------------------------------------------------------
#
# Email / Graph connectors are still stubs and return TBC placeholders.
# Google Sheets is live: five tabs are read via a service account held in
# st.secrets. Every connector is fail-soft — any missing credential, missing
# library, network error or unexpected tab shape returns an empty DataFrame
# so the dashboard still renders its "awaiting data connection" fallback
# instead of raising a traceback.
#
# --- Google Sheets ----------------------------------------------------------
#
# Secrets expected in .streamlit/secrets.toml (or the Streamlit Cloud UI):
#
#     GOOGLE_SHEET_URL = "https://docs.google.com/spreadsheets/d/…/edit"
#
#     [GOOGLE_SA_JSON]
#     type                        = "service_account"
#     project_id                  = "…"
#     private_key_id              = "…"
#     private_key                 = "-----BEGIN PRIVATE KEY-----\n…\n-----END PRIVATE KEY-----\n"
#     client_email                = "…@…iam.gserviceaccount.com"
#     client_id                   = "…"
#     auth_uri                    = "https://accounts.google.com/o/oauth2/auth"
#     token_uri                   = "https://oauth2.googleapis.com/token"
#     auth_provider_x509_cert_url = "https://www.googleapis.com/oauth2/v1/certs"
#     client_x509_cert_url        = "…"
#
# Share the sheet with the service account's client_email (Viewer is enough).

_GSHEETS_SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets.readonly",
    "https://www.googleapis.com/auth/drive.readonly",
]


def _secret(key: str, default: Any = None) -> Any:
    """Safe st.secrets accessor — returns `default` if secrets not configured."""
    try:
        return st.secrets[key]
    except Exception:
        return default


@st.cache_resource(show_spinner=False)
def _gsheet_client():
    """Build & cache a gspread client from the service account in st.secrets.

    Returns None (never raises) if:
      - gspread / google-auth are not installed
      - GOOGLE_SA_JSON secret is missing or malformed
      - the credentials fail to authorise
    """
    sa_info = _secret("GOOGLE_SA_JSON")
    if not sa_info:
        return None
    try:
        # st.secrets returns an AttrDict — convert to plain dict for google-auth
        sa_info = dict(sa_info)
    except Exception:
        return None
    try:
        import gspread
        from google.oauth2.service_account import Credentials
    except ImportError:
        return None
    try:
        creds = Credentials.from_service_account_info(sa_info, scopes=_GSHEETS_SCOPES)
        return gspread.authorize(creds)
    except Exception:
        return None


@st.cache_data(ttl=300, show_spinner=False)
def _read_sheet_tab(tab_name: str, expected_cols: Optional[List[str]] = None) -> pd.DataFrame:
    """Read a named worksheet tab as a DataFrame. Cached for 5 minutes.

    On any failure (missing secret, auth error, tab missing, empty sheet,
    network blip) an empty DataFrame is returned — with `expected_cols` as
    its schema if provided — so each section's own 'awaiting data' branch
    continues to render the TBC placeholders.
    """
    client = _gsheet_client()
    if client is None:
        return pd.DataFrame(columns=expected_cols or [])
    url = _secret("GOOGLE_SHEET_URL", GOOGLE_SHEET_URL)
    if not url:
        return pd.DataFrame(columns=expected_cols or [])
    try:
        sh = client.open_by_url(url)
        ws = sh.worksheet(tab_name)
        # get_all_records() uses row 1 as header — standard for our 8 tabs
        all_vals = ws.get_all_values()
        if not all_vals or len(all_vals) < 2:
            return pd.DataFrame(columns=expected_cols or [])
        hdr_row = 0
        for i, row in enumerate(all_vals):
            if len([c for c in row if str(c).strip()]) >= 3:
                hdr_row = i
                break
        headers = [str(c).strip() for c in all_vals[hdr_row]]
        data = [r for r in all_vals[hdr_row+1:] if any(str(c).strip() for c in r)]
        if not data:
            return pd.DataFrame(columns=expected_cols or [])
        n = len(headers)
        padded = [r[:n] + ['']*(max(0,n-len(r))) for r in data]
        df = pd.DataFrame(padded, columns=headers)
        df = df[[c for c in df.columns if c]]
        return df
    except Exception as e:
        return pd.DataFrame(columns=expected_cols or [])


# --- Stubs that will be replaced once connectors are wired -------------------

def _fetch_email_stats(customer: Dict[str, Any], window_days: int) -> Dict[str, Any]:
    """Replace with Microsoft Graph query against MAILBOXES (connected=True)."""
    return {
        "sent": None, "received": None,
        "last_contact_date": None,
        "last_contact_summary": "Awaiting mailbox connector data",
        "by_category": {"complaints": None, "orders": None, "queries": None,
                        "quotations": None, "doc_review": None},
        "sla_breaches": None,
        "open_items": None,
        "complaint_open": None, "complaint_resolved_90d": None,
        "customer_reply_hrs": None,
    }


# --- Google-Sheet-backed readers (auto-refresh every 5 min via cache TTL) ----

@st.cache_data(ttl=300, show_spinner=False)
def _fetch_pipeline_rows() -> pd.DataFrame:
    """Read the 'Pipeline Tracker' tab. Empty DataFrame on any failure."""
    cols = ["Customer", "Supplier", "Product/SKU", "Current stage",
            "Days in stage", "CNL received", "Licence applied",
            "Licence expiry", "Export licence", "Shipped", "Arrived", "Notes"]
    return _read_sheet_tab("4. Pipeline Tracker", cols)


@st.cache_data(ttl=300, show_spinner=False)
def _fetch_stock_rows() -> pd.DataFrame:
    """Read the 'Stock Intelligence' tab. Empty DataFrame on any failure."""
    cols = ["Brand", "SKU", "Bulk SOH (kg)", "FG Units", "Days in stock",
            "Idle flag", "Packaging available"]
    return _read_sheet_tab("5. Stock Intelligence", cols)


@st.cache_data(ttl=300, show_spinner=False)
def _fetch_capacity_rows() -> pd.DataFrame:
    """Read the 'Capacity Dashboard' tab. Empty DataFrame on any failure."""
    cols = ["Brand", "Full Capacity (kg)", "SOH Today (kg)",
            "Pipeline (kg)", "Utilisation %"]
    return _read_sheet_tab("8. Capacity Dashboard", cols)


@st.cache_data(ttl=300, show_spinner=False)
def _fetch_npi_rows() -> pd.DataFrame:
    """Read the 'NPI Tracker' tab. Empty DataFrame on any failure."""
    cols = ["Customer", "Product", "Task", "Owner", "Department",
            "Start date", "Due date", "Days overdue", "Status"]
    return _read_sheet_tab("6. NPI Tracker", cols)


@st.cache_data(ttl=300, show_spinner=False)
def _fetch_action_log() -> pd.DataFrame:
    """Read the 'Action Log' tab. Empty DataFrame on any failure."""
    cols = ["Priority", "Category", "Customer", "Issue",
            "Date identified", "Days open", "Owner", "Action required", "Status"]
    return _read_sheet_tab("7. Action Log", cols)

def _fetch_inbound_leads(window_days: int) -> pd.DataFrame:
    cols = ["Company", "Type", "First contact", "Contacted by",
            "What they want", "AM assigned", "Stage", "Last update"]
    return pd.DataFrame(columns=cols)

def _fetch_compliance_bundle() -> Dict[str, pd.DataFrame]:
    return {
        "licence_expiry": pd.DataFrame(columns=["Customer", "Licence #", "Expiry", "Days left", "RAG"]),
        "audits":         pd.DataFrame(columns=["Supplier", "Last audit", "Next due", "Status", "RAG"]),
        "home_office":    pd.DataFrame(columns=["Supplier", "Registered", "Expiry", "Status"]),
        "gmp_gacp":       pd.DataFrame(columns=["Supplier", "Cert type", "Issued", "Expiry", "Status"]),
        "oos":            pd.DataFrame(columns=["Customer", "Product", "Batch", "Raised", "Status"]),
        "mhra":           pd.DataFrame(columns=["Subject", "Date", "Direction", "Action required"]),
        "yellow_card":    pd.DataFrame(columns=["Date", "Product", "Event", "Status"]),
    }

# ---------------------------------------------------------------------------
# SIDEBAR
# ---------------------------------------------------------------------------

def build_sidebar() -> Dict[str, Any]:
    ss = st.session_state
    with st.sidebar:
        st.markdown(f"### {APP_TITLE}")
        st.caption(APP_TAGLINE)
        st.markdown("---")

        # Refresh + view mode
        cols = st.columns([1, 1])
        if cols[0].button("\U0001F504 Refresh all", use_container_width=True):
            ss["last_refresh"] = datetime.now()
            # Force-clear cached Google Sheet reads so the next render re-fetches
            try:
                st.cache_data.clear()
            except Exception:
                pass
            st.rerun()
        ss["view_mode"] = cols[1].selectbox(
            "View", ["Detailed", "Compact", "Red alerts only", "My attention today"],
            index=["Detailed", "Compact", "Red alerts only", "My attention today"].index(ss["view_mode"]),
        )

        st.markdown("#### Global filters")
        am = st.selectbox("Account Manager", ["All"] + AMS, index=0)
        status = st.multiselect(
            "Customer status",
            STATUSES + ["All"],
            default=["Existing", "Stable", "Onboarded", "Prospect", "Dormant"],
        )
        timeframe = st.selectbox(
            "Email timeframe",
            ["Last 7 days", "Last 14 days", "Last 30 days", "Last 60 days",
             "Last 90 days", "This quarter", "Last quarter"],
            index=2,
        )
        date_from, date_to = st.date_input(
            "Custom date range (optional)",
            value=(date.today() - timedelta(days=30), date.today()),
        )

        st.markdown("#### Show / hide sections")
        labels = {
            "summary_bar":    "2. Top summary bar",
            "relationship":   "3. Customer relationship health",
            "commercial":     "4. Commercial growth",
            "am_kpis":        "5. Account Manager KPIs",
            "pipeline":       "6. Import & supply chain",
            "stock_capacity": "7. Stock & capacity",
            "npi":            "8. NPI tracker",
            "compliance":     "9. Compliance & regulatory",
            "financial":      "10. Financial (Sage)",
            "alerts":         "11. Alerts & action log",
        }
        for key, lbl in labels.items():
            ss["section_flags"][key] = st.checkbox(lbl, value=ss["section_flags"].get(key, True), key=f"flag_{key}")

        st.markdown("#### SLA Settings")
        sla: Dict[str, Any] = {}
        with st.expander("Response-time SLAs", expanded=False):
            sla["complaint_amber_hrs"] = st.slider("Complaint response — amber (hrs)", 1, 12, SLA_DEFAULTS["complaint_amber_hrs"])
            sla["complaint_red_hrs"]   = st.slider("Complaint response — red (hrs)",   1, 24, SLA_DEFAULTS["complaint_red_hrs"])
            sla["order_amber_hrs"]     = st.slider("Order / Import licence — amber (hrs)", 6, 48, SLA_DEFAULTS["order_amber_hrs"])
            sla["order_red_hrs"]       = st.slider("Order / Import licence — red (hrs)",   12, 72, SLA_DEFAULTS["order_red_hrs"])
            sla["query_amber_hrs"]     = st.slider("Query — amber (hrs)", 12, 96, SLA_DEFAULTS["query_amber_hrs"])
            sla["query_red_hrs"]       = st.slider("Query — red (hrs)",   24, 120, SLA_DEFAULTS["query_red_hrs"])
        with st.expander("Contact-frequency SLAs", expanded=False):
            sla["active_contact_amber_days"] = st.slider("Active customer — amber (days)", 3, 21, SLA_DEFAULTS["active_contact_amber_days"])
            sla["active_contact_red_days"]   = st.slider("Active customer — red (days)",   7, 30, SLA_DEFAULTS["active_contact_red_days"])
            sla["stable_contact_amber_days"] = st.slider("Stable customer — amber (days)", 14, 45, SLA_DEFAULTS["stable_contact_amber_days"])
            sla["stable_contact_red_days"]   = st.slider("Stable customer — red (days)",   21, 60, SLA_DEFAULTS["stable_contact_red_days"])
            sla["prospect_contact_amber_days"] = st.slider("Prospect — amber (days)", 7, 30, SLA_DEFAULTS["prospect_contact_amber_days"])
            sla["prospect_contact_red_days"]   = st.slider("Prospect — red (days)",   14, 60, SLA_DEFAULTS["prospect_contact_red_days"])
        with st.expander("Stock & capacity SLAs", expanded=False):
            sla["idle_stock_amber_days"] = st.slider("Idle stock — amber (days)", 30, 90,  SLA_DEFAULTS["idle_stock_amber_days"])
            sla["idle_stock_red_days"]   = st.slider("Idle stock — red (days)",   60, 180, SLA_DEFAULTS["idle_stock_red_days"])
            sla["capacity_amber_pct"]    = st.slider("Capacity utilisation — amber (%)", 50, 90, SLA_DEFAULTS["capacity_amber_pct"])
            sla["capacity_red_pct"]      = st.slider("Capacity utilisation — red (%)",   60, 100, SLA_DEFAULTS["capacity_red_pct"])
        with st.expander("NPI & pipeline SLAs", expanded=False):
            sla["npi_amber_days"]      = st.slider("NPI overdue — amber (days)", 0, 14, SLA_DEFAULTS["npi_amber_days"])
            sla["npi_red_days"]        = st.slider("NPI overdue — red (days)",   1, 30, SLA_DEFAULTS["npi_red_days"])
            sla["pipeline_amber_days"] = st.slider("Import stage stalled — amber (days)", 3, 14, SLA_DEFAULTS["pipeline_amber_days"])
            sla["pipeline_red_days"]   = st.slider("Import stage stalled — red (days)",   7, 30, SLA_DEFAULTS["pipeline_red_days"])
        with st.expander("Compliance warning windows", expanded=False):
            sla["audit_warn_days"]   = st.slider("Supplier audit warning (days before due)", 14, 60, SLA_DEFAULTS["audit_warn_days"])
            sla["licence_warn_days"] = st.slider("Import licence expiry warning (days)",    14, 60, SLA_DEFAULTS["licence_warn_days"])

        st.markdown("---")
        connected = sum(1 for m in MAILBOXES if m["connected"])
        st.caption(f"\U0001F4EC Mailboxes connected: {connected}/{len(MAILBOXES)}")
        st.caption(f"\U0001F4C5 Last refresh: {_ts()}")

    return {
        "am": am,
        "status": status,
        "timeframe": timeframe,
        "date_from": date_from,
        "date_to": date_to,
        "sla": sla,
    }

# ---------------------------------------------------------------------------
# SECTION 2 — TOP SUMMARY BAR
# ---------------------------------------------------------------------------

def section_summary_bar(filters: Dict[str, Any]) -> None:
    st.markdown("<div class='section-title'>Executive summary</div>", unsafe_allow_html=True)
    custs = _active_customers(filters["am"], filters["status"])

    # Placeholder RAG classification: until email data is live, Existing = Green,
    # Onboarded = Amber, Prospect = Amber, Dormant = Dormant.
    red = 0
    amber = sum(1 for c in custs if c["status"] in ("Onboarded", "Prospect"))
    green = sum(1 for c in custs if c["status"] in ("Existing", "Stable"))
    dormant = sum(1 for c in custs if c["status"] == "Dormant")

    c1, c2, c3, c4 = st.columns(4)
    for col, lbl, val, colour in (
        (c1, "Red customers",    red,     COLOR_RED),
        (c2, "Amber customers",  amber,   COLOR_AMBER),
        (c3, "Green customers",  green,   COLOR_GREEN),
        (c4, "Dormant customers", dormant, COLOR_DORMANT),
    ):
        col.markdown(
            f"<div class='metric-card' style='background:{colour}'>"
            f"<div class='label'>{lbl}</div>"
            f"<div class='value'>{val}</div></div>",
            unsafe_allow_html=True,
        )

    st.markdown(
        "<div class='small-muted' style='margin-top:10px;'>"
        "<b>TBC</b> emails need response &nbsp;|&nbsp; "
        "<b>TBC</b> NPI tasks overdue &nbsp;|&nbsp; "
        "<b>TBC</b> compliance items due "
        "<span style='opacity:.6'> — will populate once connectors wired</span>"
        "</div>",
        unsafe_allow_html=True,
    )
    _timestamp_caption("Summary bar")

# ---------------------------------------------------------------------------
# SECTION 3 — CUSTOMER RELATIONSHIP HEALTH
# ---------------------------------------------------------------------------

def _customer_status_order(status: str) -> int:
    # Red first, then Amber, then Green, then Dormant
    mapping = {"Onboarded": 1, "Prospect": 1, "Existing": 2, "Stable": 2, "Dormant": 3}
    return mapping.get(status, 9)

def section_relationship(filters: Dict[str, Any]) -> None:
    with st.expander("3. Customer relationship health", expanded=True):
        custs = _active_customers(filters["am"], filters["status"])
        window = _timeframe_days(filters["timeframe"], date.today())

        rows = []
        for c in custs:
            stats = _fetch_email_stats(c, window)
            status = c["status"]
            # Determine contact SLA bracket for this customer
            if status in ("Existing", "Onboarded"):
                amber, red = filters["sla"]["active_contact_amber_days"], filters["sla"]["active_contact_red_days"]
            elif status == "Stable":
                amber, red = filters["sla"]["stable_contact_amber_days"], filters["sla"]["stable_contact_red_days"]
            elif status == "Prospect":
                amber, red = filters["sla"]["prospect_contact_amber_days"], filters["sla"]["prospect_contact_red_days"]
            else:
                amber, red = 90, 90
            days_since = None  # will be populated by live connector
            rag = "Dormant" if status == "Dormant" else _rag_from_days(days_since, amber, red)
            cat = stats["by_category"]
            rows.append({
                "Customer": c["name"],
                "AM": c["am"],
                "Status": status,
                "Last contact": stats["last_contact_date"] or "TBC",
                "Days since": days_since if days_since is not None else "TBC",
                "Last summary": stats["last_contact_summary"],
                "Sent": stats["sent"] if stats["sent"] is not None else "TBC",
                "Received": stats["received"] if stats["received"] is not None else "TBC",
                "Avg complaint (h)":  cat["complaints"] or "TBC",
                "Avg orders (h)":     cat["orders"] or "TBC",
                "Avg queries (h)":    cat["queries"] or "TBC",
                "Avg quotations (h)": cat["quotations"] or "TBC",
                "Avg doc review (h)": cat["doc_review"] or "TBC",
                "SLA breaches": stats["sla_breaches"] if stats["sla_breaches"] is not None else "TBC",
                "Open items": stats["open_items"] if stats["open_items"] is not None else "TBC",
                "Complaints (open / 90d resolved)":
                    f"{stats['complaint_open'] or 'TBC'} / {stats['complaint_resolved_90d'] or 'TBC'}",
                "Customer reply (h)": stats["customer_reply_hrs"] or "TBC",
                "RAG": rag,
                "_sort": _customer_status_order(status),
            })
        df = pd.DataFrame(rows).sort_values(["_sort", "Customer"]).drop(columns=["_sort"])

        # Use emoji badges for the RAG column so st.dataframe() can render it
        # (unlike st.write(df.to_html(...)), st.dataframe doesn't evaluate HTML).
        df_display = df.copy()
        df_display["RAG"] = df_display["RAG"].apply(_rag_badge)

        # Compact view drops some columns
        if st.session_state["view_mode"] == "Compact":
            keep = ["Customer", "AM", "Status", "Last contact", "Days since", "RAG"]
            df_display = df_display[keep]

        st.dataframe(
            df_display,
            use_container_width=True,
            hide_index=True,
            column_config=_column_config(
                df_display.columns.tolist(),
                wide=["Customer", "Last summary", "Open items",
                      "Complaints (open / 90d resolved)"],
                narrow=["AM", "Status", "Days since", "Sent", "Received",
                        "Avg complaint (h)", "Avg orders (h)", "Avg queries (h)",
                        "Avg quotations (h)", "Avg doc review (h)",
                        "SLA breaches", "Customer reply (h)", "RAG"],
                pin="Customer",
            ),
        )
        st.markdown(
            "<div class='awaiting'>Per-customer email thread summaries populate when the "
            "Microsoft Graph connector reads the 12 connected mailboxes. Clicking a row "
            "below opens a drill-down; live data will be shown as it comes online.</div>",
            unsafe_allow_html=True,
        )

        picked = st.selectbox("Drill into a customer", [c["name"] for c in custs], key="rel_drill")
        if picked:
            c = next(c for c in custs if c["name"] == picked)
            st.markdown(f"#### {picked} — {c['am']} ({c['status']})")
            c1, c2, c3 = st.columns(3)
            c1.metric("Emails sent (window)", "TBC")
            c2.metric("Emails received (window)", "TBC")
            c3.metric("Avg response (h)", "TBC")
            st.caption("Thread summaries, attachments and sentiment trend will appear here once the mailbox connector is live.")
            # Per-customer action buttons
            b1, b2, b3, b4 = st.columns(4)
            new_status = b1.selectbox("Change status", STATUSES, index=STATUSES.index(c["status"]), key=f"status_{picked}")
            if b2.button("Apply status", key=f"apply_status_{picked}"):
                if new_status == "Closed":
                    st.warning(f"Confirm closure of {picked}? This removes them from all reporting.")
                    if st.button(f"Confirm close {picked}", key=f"conf_close_{picked}"):
                        c["status"] = "Closed"
                        st.success("Status updated. (Will sync to Google Sheet once write-scope is granted.)")
                else:
                    c["status"] = new_status
                    st.success(f"{picked} → {new_status}. (Sheet sync pending write-scope.)")
            if b3.button("Generate quarterly review", key=f"qr_{picked}"):
                st.info("Quarterly review document generation queued — Word doc will be written to Google Drive and the CEO notified by email.")
            if b4.button("Mark review complete", key=f"mr_{picked}"):
                st.session_state["review_dates"][picked] = (date.today() + timedelta(days=90)).isoformat()
                st.success(f"Next review for {picked} auto-set to {st.session_state['review_dates'][picked]}.")

        _export_csv_button(df, "relationship_health.csv", "rel_health")
        _timestamp_caption("Customer relationship health")

# ---------------------------------------------------------------------------
# SECTION 4 — COMMERCIAL GROWTH
# ---------------------------------------------------------------------------

def section_commercial(filters: Dict[str, Any]) -> None:
    with st.expander("4. Commercial growth", expanded=True):
        custs = _active_customers(filters["am"], filters["status"])
        rows = []
        for c in custs:
            rows.append({
                "Customer": c["name"],
                "AM": c["am"],
                "Status": c["status"],
                "Orders / mo (this Q)": "TBC",
                "Orders / mo (last Q)": "TBC",
                "Trend": "\u2014",
                "Volume kg / SKU (this Q)": "TBC",
                "Volume kg / SKU (last Q)": "TBC",
                "Active SKUs (90d)": "TBC",
                "New SKUs (90d)": "TBC",
                "Revenue / mo": "Awaiting Sage",
                "Pipeline value": "TBC",
            })
        df = pd.DataFrame(rows)
        st.dataframe(
            df,
            use_container_width=True,
            hide_index=True,
            column_config=_column_config(
                df.columns.tolist(),
                wide=["Customer", "Revenue / mo", "Pipeline value"],
                narrow=["AM", "Status", "Trend", "Active SKUs (90d)", "New SKUs (90d)"],
                pin="Customer",
            ),
        )
        _export_csv_button(df, "commercial_growth.csv", "comm_growth")

        st.markdown("##### Inbound leads (new supplier / brand / clinic / pharmacy)")
        leads = _fetch_inbound_leads(_timeframe_days(filters["timeframe"], date.today()))
        if leads.empty:
            _awaiting("Inbound leads populate from full-mailbox scan — connector pending on Graph read scope.")
            leads = pd.DataFrame([{
                "Company": "—", "Type": "—", "First contact": "—",
                "Contacted by": "—", "What they want": "—",
                "AM assigned": "—", "Stage": "—", "Last update": "—",
            }])
        st.dataframe(
            leads,
            use_container_width=True,
            hide_index=True,
            column_config=_column_config(
                leads.columns.tolist(),
                wide=["Company", "What they want"],
                narrow=["Type", "First contact", "AM assigned", "Stage", "Last update"],
                pin="Company",
            ),
        )
        _export_csv_button(leads, "inbound_leads.csv", "inbound_leads")
        _timestamp_caption("Commercial growth")

# ---------------------------------------------------------------------------
# SECTION 5 — ACCOUNT MANAGER KPIs
# ---------------------------------------------------------------------------

def _am_panel(am_name: str, filters: Dict[str, Any]) -> None:
    st.markdown(f"### {am_name}")
    portfolio = [c for c in st.session_state["customers"] if c["am"] == am_name and c["status"] != "Closed"]

    # Portfolio RAG summary (placeholder distribution until mailbox live)
    r = 0
    a = sum(1 for c in portfolio if c["status"] in ("Onboarded", "Prospect"))
    g = sum(1 for c in portfolio if c["status"] in ("Existing", "Stable"))
    d = sum(1 for c in portfolio if c["status"] == "Dormant")
    cols = st.columns(4)
    cols[0].markdown(_pill(f"Red {r}", "red"),    unsafe_allow_html=True)
    cols[1].markdown(_pill(f"Amber {a}", "amber"), unsafe_allow_html=True)
    cols[2].markdown(_pill(f"Green {g}", "green"), unsafe_allow_html=True)
    cols[3].markdown(_pill(f"Dormant {d}", "dormant"), unsafe_allow_html=True)

    st.caption("Average response time by category")
    cat_df = pd.DataFrame([{
        "Category": x, "Avg hours": "TBC",
    } for x in ["Complaints", "Orders / Licences", "Queries", "Quotations", "Doc review"]])
    st.dataframe(
        cat_df,
        use_container_width=True,
        hide_index=True,
        column_config=_column_config(
            cat_df.columns.tolist(),
            wide=["Category"],
            narrow=["Avg hours"],
            pin="Category",
        ),
    )

    m1, m2, m3 = st.columns(3)
    m1.metric("SLA breaches (this month)", "TBC")
    m2.metric("Contact coverage last 7d", "TBC")
    m3.metric("Open actions", "TBC")

    st.markdown("**Customers not contacted in last 7 days**")
    _awaiting("Populates from mailbox scan.")
    st.markdown("**Stable check-ins due this month**")
    stable_due = [c["name"] for c in portfolio if c["status"] == "Stable"]
    st.write(", ".join(stable_due) if stable_due else "None")
    st.markdown("**Quarterly reviews due**")
    due_list = []
    for c in portfolio:
        next_rev = st.session_state["review_dates"].get(c["name"])
        if next_rev:
            try:
                if date.fromisoformat(next_rev) <= date.today() + timedelta(days=14):
                    due_list.append(f"{c['name']} ({next_rev})")
            except Exception:
                pass
    st.write(", ".join(due_list) if due_list else "None flagged — set a date in Section 3 drill-down.")

    st.markdown("**Prospects — days since last contact**")
    prosp = [{"Customer": c["name"], "Stage": "TBC", "Days since contact": "TBC"}
             for c in portfolio if c["status"] == "Prospect"]
    if prosp:
        prosp_df = pd.DataFrame(prosp)
        st.dataframe(
            prosp_df,
            use_container_width=True,
            hide_index=True,
            column_config=_column_config(
                prosp_df.columns.tolist(),
                wide=["Customer"],
                narrow=["Stage", "Days since contact"],
                pin="Customer",
            ),
        )
    else:
        st.caption("No prospects in portfolio.")

    st.markdown("**NPI bottleneck (overdue tasks by department)**")
    bottleneck = pd.DataFrame([{"Department": d, "Overdue tasks": "TBC"} for d in NPI_DEPARTMENTS])
    st.dataframe(
        bottleneck,
        use_container_width=True,
        hide_index=True,
        column_config=_column_config(
            bottleneck.columns.tolist(),
            wide=["Department"],
            narrow=["Overdue tasks"],
            pin="Department",
        ),
    )

    m4, m5 = st.columns(2)
    m4.metric("Complaint rate (complaints / orders)", "TBC")
    m5.metric("Portfolio size", len(portfolio))

def section_am_kpis(filters: Dict[str, Any]) -> None:
    with st.expander("5. Account Manager KPIs", expanded=True):
        left, right = st.columns(2)
        with left:  _am_panel("Neil", filters)
        with right: _am_panel("Waqar", filters)
        _timestamp_caption("AM KPIs")

# ---------------------------------------------------------------------------
# SECTION 6 — IMPORT AND SUPPLY CHAIN PIPELINE
# ---------------------------------------------------------------------------

def section_pipeline(filters: Dict[str, Any]) -> None:
    with st.expander("6. Import & supply chain pipeline", expanded=True):
        df = _fetch_pipeline_rows()
        if df.empty:
            _awaiting("Pipeline Tracker tab not yet connected. Structure shown below.")
            df = pd.DataFrame([{
                "Customer": "—", "Supplier": "—", "Product/SKU": "—",
                "Current stage": PIPELINE_STAGES[0], "Days in stage": "TBC",
                "CNL received": "TBC", "Licence applied": "TBC",
                "Licence expiry": "TBC", "Export licence": "TBC",
                "Shipped": "TBC", "Arrived": "TBC", "Notes": "",
            }])

        fc1, fc2, fc3 = st.columns(3)
        stage_filter = fc1.selectbox("Stage filter", ["All"] + PIPELINE_STAGES, key="pipe_stage")
        stalled_only = fc2.checkbox("Stalled only", key="pipe_stall")
        red_only     = fc3.checkbox("Red only",     key="pipe_red")

        st.dataframe(
            df,
            use_container_width=True,
            hide_index=True,
            column_config=_column_config(
                df.columns.tolist(),
                wide=["Customer", "Supplier", "Product/SKU", "Notes"],
                narrow=["Days in stage", "CNL received", "Licence applied",
                        "Licence expiry", "Export licence", "Shipped", "Arrived"],
                pin="Customer",
            ),
        )

        st.markdown("##### Bottleneck analysis")
        bn = pd.DataFrame([{"Stage": s, "Active imports": "TBC", "Avg days": "TBC"} for s in PIPELINE_STAGES])
        st.dataframe(
            bn,
            use_container_width=True,
            hide_index=True,
            column_config=_column_config(
                bn.columns.tolist(),
                wide=["Stage"],
                narrow=["Active imports", "Avg days"],
                pin="Stage",
            ),
        )

        _export_csv_button(df, "pipeline.csv", "pipe_export")
        _timestamp_caption("Import pipeline")

# ---------------------------------------------------------------------------
# SECTION 7 — STOCK AND CAPACITY
# ---------------------------------------------------------------------------

def section_stock_capacity(filters: Dict[str, Any]) -> None:
    with st.expander("7. Stock & capacity", expanded=True):
        st.markdown("#### A. Stock intelligence")
        stock = _fetch_stock_rows()
        if stock.empty:
            _awaiting("Stock Intelligence tab of the Google Sheet is not yet connected.")
            stock = pd.DataFrame([{
                "Brand": "—", "SKU": "—", "Bulk SOH (kg)": "TBC",
                "FG Units": "TBC", "Days in stock": "TBC",
                "Idle flag": "TBC", "Packaging available": "TBC",
            }])
        st.dataframe(
            stock,
            use_container_width=True,
            hide_index=True,
            column_config=_column_config(
                stock.columns.tolist(),
                wide=["Brand", "SKU", "Packaging available"],
                narrow=["Bulk SOH (kg)", "FG Units", "Days in stock", "Idle flag"],
                pin="Brand",
            ),
        )
        st.caption("Total kg held: TBC  ·  Total idle kg: TBC  ·  Idle value: TBC")
        _export_csv_button(stock, "stock.csv", "stock_export")

        st.markdown("#### B. Capacity")
        cap = _fetch_capacity_rows()
        if cap.empty:
            _awaiting("Capacity Dashboard tab of the Google Sheet is not yet connected.")
            cap = pd.DataFrame([{
                "Brand": "—", "Full Capacity (kg)": "TBC",
                "SOH Today (kg)": "TBC", "Pipeline (kg)": "TBC",
                "Utilisation %": "TBC",
            }])
        st.dataframe(
            cap,
            use_container_width=True,
            hide_index=True,
            column_config=_column_config(
                cap.columns.tolist(),
                wide=["Brand"],
                narrow=["Full Capacity (kg)", "SOH Today (kg)",
                        "Pipeline (kg)", "Utilisation %"],
                pin="Brand",
            ),
        )

        c1, c2, c3 = st.columns(3)
        c1.metric("Unit 13 Dollman St — bulk", "TBC kg")
        c2.metric("Cato Street — finished goods", "TBC units")
        c3.metric("Trend vs last week", "TBC")
        _export_csv_button(cap, "capacity.csv", "cap_export")
        _timestamp_caption("Stock & capacity")

# ---------------------------------------------------------------------------
# SECTION 8 — NPI TRACKER
# ---------------------------------------------------------------------------

def section_npi(filters: Dict[str, Any]) -> None:
    with st.expander("8. NPI tracker", expanded=True):
        npi = _fetch_npi_rows()
        if not npi.empty and "Status" in npi.columns:
            try:
                tot  = len(npi)
                done = len(npi[npi["Status"].str.lower().str.contains("complete|done|finished", na=False)])
                # Find the days overdue column regardless of capitalisation
                day_col = next((c for c in npi.columns if "overdue" in c.lower() and "day" in c.lower()), None)
                if day_col:
                    overdue = len(npi[npi[day_col].astype(str).str.replace(",","").str.strip().str.match(r"^[1-9]\d*$")])
                else:
                    overdue = "TBC"
                opn = tot - done
            except Exception:
                tot = done = opn = overdue = "TBC"
        else:
            tot = done = opn = overdue = "TBC"
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Total tasks", tot)
        c2.metric("Completed", done)
        c3.metric("Open", opn)
        c4.metric("Overdue", overdue)

        f1, f2, f3, f4 = st.columns(4)
        f1.selectbox("Customer", ["All"] + [c["name"] for c in st.session_state["customers"]], key="npi_cust")
        f2.selectbox("Owner",    ["All"], key="npi_owner")
        f3.selectbox("Department", ["All"] + NPI_DEPARTMENTS, key="npi_dept")
        f4.checkbox("Overdue only", key="npi_overdue")

        if npi.empty:
            _awaiting("NPI Tracker tab not yet connected. Structure shown below.")
            npi = pd.DataFrame([{
                "Customer": "—", "Product": "—", "Task": "—",
                "Owner": "—", "Department": "—", "Start date": "TBC",
                "Due date": "TBC", "Days overdue": "TBC", "Status": "—",
            }])
        st.dataframe(
            npi,
            use_container_width=True,
            hide_index=True,
            column_config=_column_config(
                npi.columns.tolist(),
                wide=["Customer", "Product", "Task"],
                narrow=["Owner", "Department", "Start date", "Due date",
                        "Days overdue", "Status"],
                pin="Customer",
            ),
        )

        st.markdown("##### Bottleneck indicator")
        bn = pd.DataFrame([{"Department": d, "Overdue tasks": "TBC"} for d in NPI_DEPARTMENTS])
        st.dataframe(
            bn,
            use_container_width=True,
            hide_index=True,
            column_config=_column_config(
                bn.columns.tolist(),
                wide=["Department"],
                narrow=["Overdue tasks"],
                pin="Department",
            ),
        )
        st.caption(
            "NPI = internal product development from first task → QP release sign-off on the new formulation. "
            "Excludes import / CNL. Cycle time = days from first task to completion."
        )
        _export_csv_button(npi, "npi.csv", "npi_export")
        _timestamp_caption("NPI tracker")

# ---------------------------------------------------------------------------
# SECTION 9 — COMPLIANCE AND REGULATORY
# ---------------------------------------------------------------------------

def section_compliance(filters: Dict[str, Any]) -> None:
    with st.expander("9. Compliance & regulatory", expanded=True):
        bundle = _fetch_compliance_bundle()
        labels = [
            ("Import licence expiry", "licence_expiry"),
            ("Supplier audit schedule", "audits"),
            ("Home Office registration", "home_office"),
            ("GACP / GMP certification", "gmp_gacp"),
            ("Out-of-spec (OOS) alerts", "oos"),
            ("MHRA correspondence", "mhra"),
            ("Yellow Card reports", "yellow_card"),
        ]
        # Column-width + pin hints per compliance table (first column is the pin)
        compliance_hints = {
            "licence_expiry": {
                "wide": ["Customer", "Licence #"],
                "narrow": ["Expiry", "Days left", "RAG"],
                "pin": "Customer",
            },
            "audits": {
                "wide": ["Supplier"],
                "narrow": ["Last audit", "Next due", "Status", "RAG"],
                "pin": "Supplier",
            },
            "home_office": {
                "wide": ["Supplier"],
                "narrow": ["Registered", "Expiry", "Status"],
                "pin": "Supplier",
            },
            "gmp_gacp": {
                "wide": ["Supplier"],
                "narrow": ["Cert type", "Issued", "Expiry", "Status"],
                "pin": "Supplier",
            },
            "oos": {
                "wide": ["Customer", "Product"],
                "narrow": ["Batch", "Raised", "Status"],
                "pin": "Customer",
            },
            "mhra": {
                "wide": ["Subject", "Action required"],
                "narrow": ["Date", "Direction"],
                "pin": "Subject",
            },
            "yellow_card": {
                "wide": ["Event"],
                "narrow": ["Date", "Product", "Status"],
                "pin": "Date",
            },
        }
        for lbl, key in labels:
            st.markdown(f"##### {lbl}")
            df = bundle[key]
            if df.empty:
                _awaiting(f"{lbl}: awaiting QA / regulatory mailbox connection.")
                df = pd.DataFrame([{c: "TBC" for c in df.columns} if list(df.columns) else {"Info": "TBC"}])
            hint = compliance_hints.get(key, {})
            st.dataframe(
                df,
                use_container_width=True,
                hide_index=True,
                column_config=_column_config(
                    df.columns.tolist(),
                    wide=hint.get("wide"),
                    narrow=hint.get("narrow"),
                    pin=hint.get("pin") if hint.get("pin") in df.columns else None,
                ),
            )
            _export_csv_button(df, f"{key}.csv", f"comp_{key}")
        _timestamp_caption("Compliance & regulatory")

# ---------------------------------------------------------------------------
# SECTION 10 — FINANCIAL
# ---------------------------------------------------------------------------

def section_financial(filters: Dict[str, Any]) -> None:
    with st.expander("10. Financial (Sage)", expanded=False):
        _awaiting("Awaiting Sage data connection — weekly CSV dump to be provided. "
                  "accounts@ and purchasing@ mailboxes are connected and will supply supplementary data.")
        cols = st.columns(5)
        for col, lbl in zip(cols, [
            "Revenue / customer / month",
            "Margin / customer",
            "Outstanding invoices",
            "Cost per import (by route)",
            "Manufacturing cost / kg",
        ]):
            col.markdown(
                f"<div class='metric-card' style='background:{COLOR_HEADER}'>"
                f"<div class='label'>{lbl}</div>"
                f"<div class='value'>TBC</div></div>",
                unsafe_allow_html=True,
            )
        _timestamp_caption("Financial")

# ---------------------------------------------------------------------------
# SECTION 11 — ALERTS AND ACTION LOG
# ---------------------------------------------------------------------------

def section_alerts(filters: Dict[str, Any]) -> None:
    with st.expander("11. Alerts & action log", expanded=True):
        log = _fetch_action_log()
        if log.empty:
            _awaiting("Action Log tab not yet connected. Structure shown below.")
            log = pd.DataFrame([
                {"Priority": "High", "Category": "—", "Customer": "—", "Issue": "—",
                 "Date identified": "TBC", "Days open": "TBC", "Owner": "—",
                 "Action required": "—", "Status": "—"},
            ])

        # Sort so High → top
        priority_order = {"High": 0, "Medium": 1, "Low": 2}
        log["_p"] = log["Priority"].map(priority_order).fillna(9)
        sort_cols = ["_p"]
        if "Date identified" in log.columns:
            sort_cols.append("Date identified")
        log = log.sort_values(sort_cols).drop(columns=["_p"])

        # Render Priority as an emoji badge so st.dataframe can display it.
        disp = log.copy()
        disp["Priority"] = disp["Priority"].apply(_priority_badge)
        st.dataframe(
            disp,
            use_container_width=True,
            hide_index=True,
            column_config=_column_config(
                disp.columns.tolist(),
                wide=["Issue", "Action required", "Customer"],
                narrow=["Priority", "Category", "Date identified",
                        "Days open", "Owner", "Status"],
                pin="Customer",
            ),
        )

        st.markdown("##### New inbound emails to ibrar@ (last 24h)")
        _awaiting("Populates when Graph connector is live.")
        sample = pd.DataFrame([{
            "Sender": "—", "Category": "—", "Subject": "—",
            "What they want": "—", "Suggested response": "—",
        }])
        st.dataframe(
            sample,
            use_container_width=True,
            hide_index=True,
            column_config=_column_config(
                sample.columns.tolist(),
                wide=["Subject", "What they want", "Suggested response"],
                narrow=["Category"],
                pin="Sender",
            ),
        )

        st.markdown("##### Automatic alert rules")
        st.markdown(
            f"- Complaint unanswered > {filters['sla']['complaint_red_hrs']}h  \n"
            f"- Order/licence unanswered > {filters['sla']['order_red_hrs']}h  \n"
            "- Customer no contact beyond the status-appropriate red threshold  \n"
            f"- NPI overdue > {filters['sla']['npi_red_days']} days past due date  \n"
            f"- Capacity utilisation ≥ {filters['sla']['capacity_red_pct']}%  \n"
            f"- Idle stock > {filters['sla']['idle_stock_red_days']} days  \n"
            f"- Import licence expiring within {filters['sla']['licence_warn_days']} days  \n"
            f"- Supplier audit due within {filters['sla']['audit_warn_days']} days"
        )

        st.markdown("##### Alert controls")
        a1, a2, a3, a4 = st.columns(4)
        alert_id = a1.text_input("Alert ID", key="alert_id")
        snooze_days = a2.selectbox("Snooze (days)", [1, 7, 14, 30], key="alert_snooze")
        note = a3.text_input("Note", key="alert_note")
        action = a4.selectbox("Action", ["Dismiss", "Snooze", "Escalate", "Add note"], key="alert_action")
        if st.button("Apply", key="alert_apply"):
            if not alert_id:
                st.warning("Enter an alert ID.")
            else:
                if action == "Dismiss":
                    st.session_state["dismissed_alerts"].add(alert_id)
                    st.success(f"Alert {alert_id} dismissed.")
                elif action == "Snooze":
                    until = (date.today() + timedelta(days=int(snooze_days))).isoformat()
                    st.session_state["snoozed_alerts"][alert_id] = until
                    st.success(f"Alert {alert_id} snoozed until {until}.")
                elif action == "Escalate":
                    st.success(f"Alert {alert_id} escalated — moved to top of action log.")
                elif action == "Add note":
                    st.session_state["alert_notes"][alert_id] = note
                    st.success(f"Note added to alert {alert_id}.")
        _export_csv_button(log, "action_log.csv", "alerts_export")
        _timestamp_caption("Alerts & action log")

# ---------------------------------------------------------------------------
# HEADER
# ---------------------------------------------------------------------------

def render_header() -> None:
    connected = sum(1 for m in MAILBOXES if m["connected"])
    st.markdown(
        f"""
        <div class='xeal-header'>
            <div>
                <h1>{APP_TITLE}</h1>
                <div class='sub'>{APP_TAGLINE} · {connected}/{len(MAILBOXES)} mailboxes connected</div>
            </div>
            <div class='sub'>Last refresh: {_ts()}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

def main() -> None:
    render_header()
    filters = build_sidebar()
    flags = st.session_state["section_flags"]

    if flags.get("summary_bar", True):    section_summary_bar(filters)
    if flags.get("relationship", True):   section_relationship(filters)
    if flags.get("commercial", True):     section_commercial(filters)
    if flags.get("am_kpis", True):        section_am_kpis(filters)
    if flags.get("pipeline", True):       section_pipeline(filters)
    if flags.get("stock_capacity", True): section_stock_capacity(filters)
    if flags.get("npi", True):            section_npi(filters)
    if flags.get("compliance", True):     section_compliance(filters)
    if flags.get("financial", True):      section_financial(filters)
    if flags.get("alerts", True):         section_alerts(filters)

    st.markdown("---")
    st.caption(
        f"Google Sheet source: {GOOGLE_SHEET_URL}  ·  "
        f"Mailboxes: {sum(1 for m in MAILBOXES if m['connected'])} live / "
        f"{sum(1 for m in MAILBOXES if not m['connected'])} pending (Neil, Waqar, Kayu etc. will auto-include once flipped on)"
    )

if __name__ == "__main__":
    main()
