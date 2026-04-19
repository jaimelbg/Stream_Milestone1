import io
import os
from datetime import datetime

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
from streamlit_autorefresh import st_autorefresh

load_dotenv()

# ── Azure config ──────────────────────────────────────────────────────────────
ACCOUNT_NAME = os.getenv("AZURE_STORAGE_ACCOUNT", "iesstsabbadbaa")
ACCOUNT_KEY  = os.getenv("AZURE_STORAGE_KEY", "")
CONTAINER    = os.getenv("AZURE_CONTAINER", "group03output")
ORDERS_PREFIX   = "stream-output/orders/"
COURIERS_PREFIX = "stream-output/couriers/"

ZONES = ["downtown", "midtown", "brooklyn", "queens", "suburbs"]

STATUS_COLORS = {
    "ONLINE_DELIVERING": "#3B82F6",
    "ONLINE_IDLE":       "#10B981",
    "ONLINE_PICKUP":     "#F59E0B",
    "OFFLINE":           "#EF4444",
}

CHART_LAYOUT = dict(
    template="plotly_white",
    font=dict(family="Inter, sans-serif", size=12, color="#374151"),
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    margin=dict(t=16, b=16, l=8, r=90),
    xaxis=dict(showgrid=False, linecolor="#E5E7EB"),
    yaxis=dict(gridcolor="#F3F4F6", linecolor="#E5E7EB"),
    legend=dict(bgcolor="rgba(0,0,0,0)", font=dict(size=11)),
)

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Food Delivery — Live Dashboard",
    page_icon="🚴",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Global CSS ────────────────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');

/* Hide only what we need to hide */
#MainMenu { visibility: hidden; }
footer { visibility: hidden; }
[data-testid="stDeployButton"] { display: none !important; }
[data-testid="stToolbar"] { display: none !important; }
[data-testid="stStatusWidget"] { display: none !important; }

/* Style the header bar dark with dot pattern */
[data-testid="stHeader"] {
    background-color: #0F172A !important;
    background-image: radial-gradient(circle, rgba(255,255,255,0.06) 1px, transparent 1px) !important;
    background-size: 18px 18px !important;
    border-bottom: 1px solid #334155 !important;
}

/* Force sidebar permanently open — override any stored browser state */
[data-testid="stSidebar"] {
    display: flex !important;
    transform: none !important;
    visibility: visible !important;
    width: 200px !important;
    min-width: 200px !important;
}
[data-testid="stSidebar"] > div:first-child {
    width: 200px !important;
    min-width: 200px !important;
    padding: 1rem 0.75rem !important;
}
/* Hide collapse/expand buttons so it can never be toggled */
[data-testid="stSidebarCollapseButton"] { display: none !important; }
[data-testid="collapsedControl"] { display: none !important; }

/* Global font */
html, body, [class*="css"] { font-family: 'Inter', sans-serif; }

/* Page background */
.stApp { background-color: #F1F5F9; }

/* ── Sidebar ── */
[data-testid="stSidebar"] {
    background-color: #0F172A;
    background-image:
        radial-gradient(circle, rgba(255,255,255,0.06) 1px, transparent 1px),
        linear-gradient(180deg, #0F172A 0%, #1E293B 100%);
    background-size: 18px 18px, 100% 100%;
    border-right: 1px solid #334155;
}
[data-testid="stSidebar"] .stMarkdown,
[data-testid="stSidebar"] label,
[data-testid="stSidebar"] p,
[data-testid="stSidebar"] span {
    color: #CBD5E1 !important;
}
[data-testid="stSidebar"] h1 {
    color: #F8FAFC !important;
    font-size: 1.1rem !important;
    font-weight: 700 !important;
    letter-spacing: 0.05em;
    padding-bottom: 0.5rem;
    border-bottom: 1px solid #334155;
    margin-bottom: 1rem !important;
}
[data-testid="stSidebar"] .stButton > button {
    background: #3B82F6;
    color: white;
    border: none;
    border-radius: 8px;
    font-weight: 600;
    width: 100%;
    padding: 0.5rem;
    transition: background 0.2s;
}
[data-testid="stSidebar"] .stButton > button:hover { background: #2563EB; }

/* ── KPI metric cards ── */
[data-testid="stMetric"] {
    background: white;
    border-radius: 12px;
    padding: 20px 24px !important;
    box-shadow: 0 1px 3px rgba(0,0,0,0.06), 0 1px 2px rgba(0,0,0,0.04);
    border-top: 3px solid #3B82F6;
}
[data-testid="stMetricLabel"] {
    font-size: 0.72rem !important;
    font-weight: 600 !important;
    text-transform: uppercase;
    letter-spacing: 0.08em;
    color: #6B7280 !important;
}
[data-testid="stMetricValue"] {
    font-size: 1.75rem !important;
    font-weight: 700 !important;
    color: #0F172A !important;
}

/* ── Chart containers ── */
[data-testid="stPlotlyChart"] {
    background: white;
    border-radius: 12px;
    padding: 16px;
    box-shadow: 0 1px 3px rgba(0,0,0,0.06);
}

/* ── Divider ── */
hr { border-color: #E2E8F0 !important; margin: 2rem 0 !important; }

/* ── Info boxes ── */
[data-testid="stInfoBox"] { border-radius: 8px; }

/* ── Dataframe ── */
[data-testid="stDataFrame"] {
    background: white;
    border-radius: 12px;
    box-shadow: 0 1px 3px rgba(0,0,0,0.06);
}
</style>
""", unsafe_allow_html=True)

# ── Helper: section header ────────────────────────────────────────────────────
def section_header(title: str, subtitle: str = ""):
    sub = f'<p style="margin:4px 0 0; font-size:0.8rem; color:#6B7280; font-weight:400;">{subtitle}</p>' if subtitle else ""
    st.markdown(f"""
    <div style="margin: 8px 0 20px;">
        <h3 style="margin:0; font-size:1rem; font-weight:700; color:#0F172A;
                   text-transform:uppercase; letter-spacing:0.06em;
                   padding-left:10px; border-left:4px solid #3B82F6;">
            {title}
        </h3>
        {sub}
    </div>
    """, unsafe_allow_html=True)


def apply_chart_style(fig, height=300):
    fig.update_layout(height=height, **CHART_LAYOUT)
    return fig

def insight(text: str, color: str = "#6B7280"):
    st.markdown(
        f"<p style='font-size:0.75rem;color:{color};margin:4px 0 12px;"
        f"padding:6px 10px;background:{'#FEF3C7' if color=='#92400E' else '#F8FAFC'};"
        f"border-left:3px solid {color};border-radius:0 6px 6px 0;'>"
        f"💡 {text}</p>",
        unsafe_allow_html=True,
    )


# ── Sidebar — controls (filters collected here, status injected later) ────────
with st.sidebar:
    st.markdown("""
    <div style="padding:12px 0 16px;">
        <div style="display:flex;align-items:center;gap:12px;margin-bottom:10px;">
            <div style="background:#3B82F6;border-radius:12px;width:48px;height:48px;
                        display:flex;align-items:center;justify-content:center;
                        font-size:1.6rem;flex-shrink:0;">🚴</div>
            <div>
                <p style="margin:0;font-size:0.95rem;font-weight:700;color:#F8FAFC;
                           letter-spacing:-0.01em;">StreamDelivery</p>
                <p style="margin:2px 0 0;font-size:0.7rem;color:#94A3B8;">Live Analytics</p>
            </div>
        </div>
        <div style="background:#1E293B;border-radius:8px;padding:8px 10px;">
            <p style="margin:0;font-size:0.7rem;font-weight:600;color:#94A3B8;
                      letter-spacing:0.04em;">GROUP 3 · IE UNIVERSITY</p>
            <p style="margin:2px 0 0;font-size:0.68rem;color:#64748B;">BBA / DBA</p>
        </div>
    </div>
    <hr style="border-color:#334155;margin:0 0 16px;">
    """, unsafe_allow_html=True)

    st.markdown("<p style='font-size:0.65rem;font-weight:700;color:#64748B;letter-spacing:0.1em;margin-bottom:8px;'>FILTERS</p>", unsafe_allow_html=True)
    selected_zones     = st.multiselect("Zones", ZONES, default=ZONES)
    hours_back         = st.slider("Hours of data", 1, 72, 24)

    st.markdown("<hr style='border-color:#334155;margin:16px 0;'>", unsafe_allow_html=True)
    st.markdown("<p style='font-size:0.65rem;font-weight:700;color:#64748B;letter-spacing:0.1em;margin-bottom:8px;'>SETTINGS</p>", unsafe_allow_html=True)
    refresh_interval_s = st.selectbox("Auto-refresh interval", [30, 60, 120], index=0,
                                       format_func=lambda x: f"Every {x}s")

    st.markdown("<hr style='border-color:#334155;margin:16px 0;'>", unsafe_allow_html=True)
    if st.button("⟳  Refresh Now", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

# ── Auto-refresh ──────────────────────────────────────────────────────────────
st_autorefresh(interval=refresh_interval_s * 1000, key="main_refresh")


# ── Data loading ──────────────────────────────────────────────────────────────
@st.cache_data(ttl=300, show_spinner=False)
def load_blob_parquet(prefix: str, max_files: int = 200) -> pd.DataFrame:
    if not ACCOUNT_KEY:
        st.error("AZURE_STORAGE_KEY is not set in .env")
        return pd.DataFrame()
    try:
        svc = BlobServiceClient(
            account_url=f"https://{ACCOUNT_NAME}.blob.core.windows.net",
            credential=ACCOUNT_KEY,
        )
        cc    = svc.get_container_client(CONTAINER)
        blobs = sorted(
            [b for b in cc.list_blobs(name_starts_with=prefix)
             if b.name.endswith(".parquet") and b.size > 0],
            key=lambda b: b.last_modified,
            reverse=True,
        )[:max_files]
        frames = []
        for blob in blobs:
            raw = cc.get_blob_client(blob.name).download_blob().readall()
            frames.append(pd.read_parquet(io.BytesIO(raw)))
        return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    except Exception as exc:
        st.error(f"Azure error: {exc}")
        return pd.DataFrame()


def prep_orders(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    df = df.copy()
    if "is_duplicate" in df.columns:
        df = df[df["is_duplicate"] != True]
    df["event_time"] = pd.to_datetime(df["event_time"], unit="ms", utc=True)
    df["hour"]       = df["event_time"].dt.floor("h")
    return df


def prep_couriers(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    df = df.copy()
    if "is_duplicate" in df.columns:
        df = df[df["is_duplicate"] != True]
    df["event_time"] = pd.to_datetime(df["event_time"], unit="ms", utc=True)
    return df


# ── Load & filter ─────────────────────────────────────────────────────────────
with st.spinner("Loading data from Azure Blob…"):
    orders_raw   = load_blob_parquet(ORDERS_PREFIX)
    couriers_raw = load_blob_parquet(COURIERS_PREFIX)

orders   = prep_orders(orders_raw)
couriers = prep_couriers(couriers_raw)
cutoff   = pd.Timestamp.now(tz="UTC") - pd.Timedelta(hours=hours_back)

if not orders.empty:
    orders = orders[orders["zone_id"].isin(selected_zones) & (orders["event_time"] >= cutoff)]
if not couriers.empty:
    couriers = couriers[couriers["zone_id"].isin(selected_zones) & (couriers["event_time"] >= cutoff)]

placed    = orders[orders["event_type"] == "ORDER_PLACED"]    if not orders.empty else pd.DataFrame()
delivered = orders[orders["event_type"] == "ORDER_DELIVERED"] if not orders.empty else pd.DataFrame()
cancelled = orders[orders["event_type"] == "ORDER_CANCELLED"] if not orders.empty else pd.DataFrame()

# ── Sidebar — pipeline status (rendered after data loads) ─────────────────────
latest_event = orders["event_time"].max() if not orders.empty else None
data_age_min = int((pd.Timestamp.now(tz="UTC") - latest_event).total_seconds() / 60) if latest_event else None
status_color = "#10B981" if data_age_min is not None and data_age_min < 60 else "#F59E0B" if data_age_min is not None else "#EF4444"
status_label = "Live" if data_age_min is not None and data_age_min < 60 else "Stale" if data_age_min is not None else "No data"



# ── Header ────────────────────────────────────────────────────────────────────
st.markdown(f"""
<div style="
    background-color: #0F172A;
    background-image:
        radial-gradient(circle, rgba(255,255,255,0.06) 1px, transparent 1px),
        linear-gradient(135deg, #0F172A 0%, #1E3A5F 100%);
    background-size: 18px 18px, 100% 100%;
    border-radius:16px; padding:28px 36px; margin-bottom:28px;
    display:flex; justify-content:space-between; align-items:center;
    flex-wrap:wrap; gap:16px;">
    <div>
        <h1 style="margin:0; color:#F8FAFC; font-size:1.6rem; font-weight:700;
                   letter-spacing:-0.02em;">
            Food Delivery — Live Analytics
        </h1>
        <p style="margin:6px 0 0; color:#94A3B8; font-size:0.82rem;">
            Real-time pipeline · Azure Event Hubs + Spark Structured Streaming
        </p>
    </div>
    <div style="text-align:right; min-width:190px;
                background:rgba(255,255,255,0.05); border-radius:10px;
                padding:12px 16px; border:1px solid rgba(255,255,255,0.08);">
        <p style="margin:0; color:#94A3B8; font-size:0.68rem; font-weight:700;
                  letter-spacing:0.08em; text-transform:uppercase;">Last Refreshed</p>
        <p style="margin:4px 0 0; color:#F8FAFC; font-size:0.88rem; font-weight:600;">
            {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC
        </p>
        <p style="margin:6px 0 0; color:#64748B; font-size:0.7rem;">
            Showing last <span style="color:#94A3B8;font-weight:600;">{hours_back}h</span>
            &nbsp;·&nbsp;
            refresh every <span style="color:#94A3B8;font-weight:600;">{refresh_interval_s}s</span>
        </p>
    </div>
</div>
""", unsafe_allow_html=True)

# ── Pipeline status strip ─────────────────────────────────────────────────────
status_bg    = "rgba(16,185,129,0.12)"  if status_label == "Live"    else "rgba(245,158,11,0.12)"  if status_label == "Stale" else "rgba(239,68,68,0.12)"
status_dot   = "#10B981"               if status_label == "Live"    else "#F59E0B"                 if status_label == "Stale" else "#EF4444"
age_display  = f"{data_age_min} min ago" if data_age_min is not None else "—"

st.markdown(f"""
<div style="display:flex; gap:12px; margin-bottom:24px; flex-wrap:wrap;">
    <div style="flex:1; min-width:150px; background:white; border-radius:10px;
                padding:12px 18px; box-shadow:0 1px 3px rgba(0,0,0,0.06);
                display:flex; align-items:center; gap:12px; border-left:3px solid {status_dot};">
        <div style="width:10px;height:10px;border-radius:50%;background:{status_dot};
                    box-shadow:0 0 6px {status_dot};flex-shrink:0;"></div>
        <div>
            <p style="margin:0;font-size:0.68rem;font-weight:700;color:#6B7280;
                      text-transform:uppercase;letter-spacing:0.07em;">Data Feed</p>
            <p style="margin:2px 0 0;font-size:0.9rem;font-weight:700;color:{status_dot};">
                {status_label}
            </p>
        </div>
    </div>
    <div style="flex:1; min-width:150px; background:white; border-radius:10px;
                padding:12px 18px; box-shadow:0 1px 3px rgba(0,0,0,0.06);
                border-left:3px solid #64748B;">
        <p style="margin:0;font-size:0.68rem;font-weight:700;color:#6B7280;
                  text-transform:uppercase;letter-spacing:0.07em;">Last Event</p>
        <p style="margin:2px 0 0;font-size:0.9rem;font-weight:700;color:#0F172A;">{age_display}</p>
    </div>
    <div style="flex:1; min-width:150px; background:white; border-radius:10px;
                padding:12px 18px; box-shadow:0 1px 3px rgba(0,0,0,0.06);
                border-left:3px solid #3B82F6;">
        <p style="margin:0;font-size:0.68rem;font-weight:700;color:#6B7280;
                  text-transform:uppercase;letter-spacing:0.07em;">Order Events</p>
        <p style="margin:2px 0 0;font-size:0.9rem;font-weight:700;color:#0F172A;">
            {len(orders_raw):,}
        </p>
    </div>
    <div style="flex:1; min-width:150px; background:white; border-radius:10px;
                padding:12px 18px; box-shadow:0 1px 3px rgba(0,0,0,0.06);
                border-left:3px solid #8B5CF6;">
        <p style="margin:0;font-size:0.68rem;font-weight:700;color:#6B7280;
                  text-transform:uppercase;letter-spacing:0.07em;">Courier Events</p>
        <p style="margin:2px 0 0;font-size:0.9rem;font-weight:700;color:#0F172A;">
            {len(couriers_raw):,}
        </p>
    </div>
    <div style="flex:1; min-width:150px; background:white; border-radius:10px;
                padding:12px 18px; box-shadow:0 1px 3px rgba(0,0,0,0.06);
                border-left:3px solid #0EA5E9;">
        <p style="margin:0;font-size:0.68rem;font-weight:700;color:#6B7280;
                  text-transform:uppercase;letter-spacing:0.07em;">Source</p>
        <p style="margin:2px 0 0;font-size:0.9rem;font-weight:700;color:#0F172A;">
            Azure Blob
        </p>
        <p style="margin:1px 0 0;font-size:0.68rem;color:#94A3B8;">{CONTAINER}</p>
    </div>
</div>
""", unsafe_allow_html=True)

# ── KPI row ───────────────────────────────────────────────────────────────────
total_orders    = len(placed)
total_revenue   = placed["order_value_cents"].sum() / 100 if not placed.empty else 0
cancel_rate     = len(cancelled) / max(total_orders, 1) * 100
active_couriers = (
    couriers[couriers["courier_status"] == "ONLINE_DELIVERING"]["courier_id"].nunique()
    if not couriers.empty else 0
)
avg_delivery_s = (
    delivered["delivery_duration_seconds"].mean()
    if not delivered.empty and "delivery_duration_seconds" in delivered.columns else None
)

k1, k2, k3, k4, k5 = st.columns(5)
k1.metric("Orders Placed",     f"{total_orders:,}")
k2.metric("Revenue",           f"€{total_revenue:,.0f}")
k3.metric("Cancellation Rate", f"{cancel_rate:.1f}%")
k4.metric("Active Couriers",   f"{active_couriers:,}")
k5.metric("Avg Delivery Time", f"{avg_delivery_s/60:.1f} min" if avg_delivery_s else "—")

st.markdown("<br>", unsafe_allow_html=True)


# ─────────────────────────────────────────────────────────────────────────────
# USE CASE 1 — Basic Windowed KPIs
# ─────────────────────────────────────────────────────────────────────────────
section_header("Use Case 1 — Basic Windowed KPIs", "Tumbling 1-hour windows · order volume & revenue over time")

col1, col2 = st.columns(2)

with col1:
    st.markdown("<p style='font-size:0.8rem;font-weight:600;color:#374151;margin-bottom:4px;'>Orders per Hour</p>", unsafe_allow_html=True)
    if not placed.empty:
        hourly = placed.groupby("hour").size().reset_index(name="orders")
        fig = px.bar(hourly, x="hour", y="orders",
                     color_discrete_sequence=["#3B82F6"])
        fig.update_traces(marker_line_width=0)
        st.plotly_chart(apply_chart_style(fig, 300), use_container_width=True)
    else:
        st.info("No order data in selected window.")

with col2:
    st.markdown("<p style='font-size:0.8rem;font-weight:600;color:#374151;margin-bottom:4px;'>Revenue per Hour (€)</p>", unsafe_allow_html=True)
    if not placed.empty:
        rev = placed.groupby("hour")["order_value_cents"].sum().reset_index()
        rev["revenue"] = rev["order_value_cents"] / 100
        fig = px.area(rev, x="hour", y="revenue",
                      color_discrete_sequence=["#10B981"])
        fig.update_traces(line_width=2)
        st.plotly_chart(apply_chart_style(fig, 300), use_container_width=True)
    else:
        st.info("No order data in selected window.")

col3, col4 = st.columns(2)

with col3:
    st.markdown("<p style='font-size:0.8rem;font-weight:600;color:#374151;margin-bottom:4px;'>Orders by Zone</p>", unsafe_allow_html=True)
    if not placed.empty:
        zone_cnt = placed.groupby("zone_id").size().reset_index(name="orders").sort_values("orders")
        fig = px.bar(zone_cnt, x="orders", y="zone_id", orientation="h",
                     color="orders", color_continuous_scale=["#DBEAFE", "#1D4ED8"])
        fig.update_coloraxes(showscale=False)
        fig.update_traces(marker_line_width=0)
        st.plotly_chart(apply_chart_style(fig, 300), use_container_width=True)
    else:
        st.info("No data.")

with col4:
    st.markdown("<p style='font-size:0.8rem;font-weight:600;color:#374151;margin-bottom:4px;'>Cancellation Reasons</p>", unsafe_allow_html=True)
    if not cancelled.empty and "cancellation_reason" in cancelled.columns:
        reasons = cancelled["cancellation_reason"].value_counts().reset_index()
        reasons.columns = ["reason", "count"]
        fig = px.pie(reasons, names="reason", values="count", hole=0.5,
                     color_discrete_sequence=["#3B82F6","#10B981","#F59E0B","#EF4444","#8B5CF6"])
        fig.update_traces(textposition="inside", textinfo="percent")
        fig.update_layout(legend=dict(orientation="h", yanchor="bottom", y=-0.3,
                                      font=dict(size=10)))
        st.plotly_chart(apply_chart_style(fig, 300), use_container_width=True)
    else:
        st.info("No cancellation data.")

st.divider()


# ─────────────────────────────────────────────────────────────────────────────
# USE CASE 2 — Demand-Supply & Courier Sessions
# ─────────────────────────────────────────────────────────────────────────────
section_header("Use Case 2 — Demand-Supply Health & Courier Sessions", "Stateful analysis · zone balance, SLA monitoring, fleet activity")

col5, col6 = st.columns(2)

with col5:
    st.markdown("<p style='font-size:0.8rem;font-weight:600;color:#374151;margin-bottom:4px;'>Orders Awaiting Pickup vs. Idle Couriers by Zone</p>", unsafe_allow_html=True)
    if not orders.empty and not couriers.empty:
        awaiting = orders[orders["event_type"] == "COURIER_ASSIGNED"].groupby("zone_id").size().rename("Orders awaiting")
        idle     = couriers[couriers["courier_status"] == "ONLINE_IDLE"].groupby("zone_id")["courier_id"].nunique().rename("Idle couriers")
        ds = pd.concat([awaiting, idle], axis=1).fillna(0).reset_index()
        fig = go.Figure()
        fig.add_bar(x=ds["zone_id"], y=ds["Orders awaiting"], name="Orders awaiting", marker_color="#3B82F6", marker_line_width=0)
        fig.add_bar(x=ds["zone_id"], y=ds["Idle couriers"],   name="Idle couriers",   marker_color="#10B981", marker_line_width=0)
        fig.update_layout(barmode="group", height=300, **CHART_LAYOUT)
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Insufficient data.")

with col6:
    st.markdown("<p style='font-size:0.8rem;font-weight:600;color:#374151;margin-bottom:4px;'>Courier Fleet Status by Zone</p>", unsafe_allow_html=True)
    if not couriers.empty:
        status_zone = couriers.groupby(["zone_id", "courier_status"])["courier_id"].nunique().reset_index(name="couriers")
        fig = px.bar(status_zone, x="zone_id", y="couriers", color="courier_status",
                     barmode="stack", color_discrete_map=STATUS_COLORS)
        fig.update_traces(marker_line_width=0)
        st.plotly_chart(apply_chart_style(fig, 300), use_container_width=True)
    else:
        st.info("No courier data.")

col7, col8 = st.columns(2)

with col7:
    st.markdown("<p style='font-size:0.8rem;font-weight:600;color:#374151;margin-bottom:4px;'>Restaurant SLA — Prep Time Percentiles by Zone (p50 / p90)</p>", unsafe_allow_html=True)
    if not orders.empty:
        preparing = orders[orders["event_type"] == "ORDER_PREPARING"][["order_id", "zone_id", "event_time"]].rename(columns={"event_time": "t_preparing"})
        picked_up = orders[orders["event_type"] == "COURIER_PICKED_UP"][["order_id", "event_time"]].rename(columns={"event_time": "t_picked_up"})
        prep_times = preparing.merge(picked_up, on="order_id", how="inner")
        prep_times["prep_min"] = (prep_times["t_picked_up"] - prep_times["t_preparing"]).dt.total_seconds() / 60
        prep_times = prep_times[(prep_times["prep_min"] > 0) & (prep_times["prep_min"] < 60)]
        if not prep_times.empty:
            sla = prep_times.groupby("zone_id")["prep_min"].quantile([0.50, 0.90]).unstack().rename(columns={0.50: "p50", 0.90: "p90"}).reset_index()
            fig = go.Figure()
            fig.add_bar(x=sla["zone_id"], y=sla["p50"], name="p50 — median", marker_color="#3B82F6", marker_line_width=0)
            fig.add_bar(x=sla["zone_id"], y=sla["p90"], name="p90 — worst",  marker_color="#F59E0B", marker_line_width=0)
            fig.update_layout(barmode="group", yaxis_title="minutes", height=300, **CHART_LAYOUT)
            st.plotly_chart(fig, use_container_width=True)
            worst_sla = sla.loc[(sla["p90"] - sla["p50"]).idxmax()]
            insight(
                f"{worst_sla['zone_id'].capitalize()} shows the widest p50→p90 gap "
                f"({worst_sla['p50']:.0f}→{worst_sla['p90']:.0f} min), indicating inconsistent kitchen prep times."
            )
        else:
            st.info("No prep time data.")
    else:
        st.info("No order data.")

with col8:
    st.markdown("<p style='font-size:0.8rem;font-weight:600;color:#374151;margin-bottom:4px;'>Active Couriers Over Time (Delivering)</p>", unsafe_allow_html=True)
    if not couriers.empty:
        delivering = (
            couriers[couriers["courier_status"] == "ONLINE_DELIVERING"]
            .assign(window=lambda d: d["event_time"].dt.floor("15min"))
            .groupby("window")["courier_id"].nunique()
            .reset_index(name="couriers_delivering")
        )
        if not delivering.empty:
            fig = px.line(delivering, x="window", y="couriers_delivering",
                          color_discrete_sequence=["#3B82F6"])
            fig.update_traces(line_width=2, fill="tozeroy", fillcolor="rgba(59,130,246,0.08)")
            fig.update_layout(yaxis_title="couriers", height=300, **CHART_LAYOUT)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No delivery activity data.")
    else:
        st.info("No courier data.")

st.divider()


# ─────────────────────────────────────────────────────────────────────────────
# USE CASE 3 — Anomaly & Fraud Detection
# ─────────────────────────────────────────────────────────────────────────────
section_header("Use Case 3 — Anomaly & Fraud Detection", "Advanced · speed anomalies, cancellation fraud signal, zone surge score")

col9, col10 = st.columns(2)

with col9:
    st.markdown("<p style='font-size:0.8rem;font-weight:600;color:#374151;margin-bottom:4px;'>Courier Speed Distribution — Anomaly Detection</p>", unsafe_allow_html=True)
    if not couriers.empty and "speed_kmh" in couriers.columns:
        normal    = couriers[couriers["speed_kmh"] <= 150]
        anomalous = couriers[couriers["speed_kmh"] > 150]
        fig = go.Figure()
        fig.add_histogram(x=normal["speed_kmh"],    name="Normal",         nbinsx=40,
                          marker_color="#3B82F6", marker_line_width=0, opacity=0.8)
        fig.add_histogram(x=anomalous["speed_kmh"], name="Anomalous",      nbinsx=20,
                          marker_color="#EF4444", marker_line_width=0, opacity=0.9)
        fig.add_vline(x=150, line_dash="dash", line_color="#EF4444",
                      annotation_text="150 km/h", annotation_font_color="#EF4444")
        fig.update_layout(barmode="overlay", xaxis_title="speed (km/h)", height=300, **CHART_LAYOUT)
        st.plotly_chart(fig, use_container_width=True)
        pct = len(anomalous) / max(len(couriers), 1) * 100
        insight(
            f"{len(anomalous):,} pings ({pct:.1f}%) exceeded 150 km/h — physically impossible for any vehicle. "
            f"Likely GPS errors or spoofed location data requiring pipeline filtering.",
            color="#B91C1C" if pct > 3 else "#92400E" if pct > 1 else "#6B7280",
        )
    else:
        st.info("No speed data.")

with col10:
    st.markdown("<p style='font-size:0.8rem;font-weight:600;color:#374151;margin-bottom:4px;'>Cancellation Rate by Zone — Fraud Signal</p>", unsafe_allow_html=True)
    if not orders.empty:
        placed_z    = orders[orders["event_type"] == "ORDER_PLACED"].groupby("zone_id").size().rename("placed")
        cancelled_z = orders[orders["event_type"] == "ORDER_CANCELLED"].groupby("zone_id").size().rename("cancelled")
        fraud = pd.concat([placed_z, cancelled_z], axis=1).fillna(0).reset_index()
        fraud["cancel_%"] = fraud["cancelled"] / fraud["placed"].replace(0, 1) * 100
        fig = px.bar(fraud, x="zone_id", y="cancel_%",
                     color="cancel_%",
                     color_continuous_scale=["#DCFCE7", "#FEF9C3", "#FEE2E2"],
                     range_color=[0, 20])
        fig.add_hline(y=10, line_dash="dash", line_color="#EF4444",
                      annotation_text="10% threshold", annotation_font_color="#EF4444",
                      annotation_position="top left")
        fig.update_traces(marker_line_width=0)
        fig.update_coloraxes(showscale=False)
        fig.update_layout(yaxis_title="cancellation %", height=300, **CHART_LAYOUT)
        st.plotly_chart(fig, use_container_width=True)
        worst = fraud.loc[fraud["cancel_%"].idxmax()]
        insight(
            f"{worst['zone_id'].capitalize()} has the highest cancellation rate ({worst['cancel_%']:.1f}%). "
            f"Zones above 10% signal potential fraud or chronic supply shortages.",
            color="#92400E" if fraud["cancel_%"].max() > 10 else "#6B7280",
        )
    else:
        st.info("No order data.")

# Zone Surge Score
st.markdown("<p style='font-size:0.8rem;font-weight:600;color:#374151;margin:16px 0 4px;'>Zone Surge Score — Orders vs. Available Couriers (most recent 30-min window)</p>", unsafe_allow_html=True)
st.markdown("<p style='font-size:0.75rem;color:#6B7280;margin-bottom:12px;'>Score = orders placed ÷ idle couriers per zone. Signals where demand is outpacing supply.</p>", unsafe_allow_html=True)

if not orders.empty and not couriers.empty:
    latest_ts    = orders["event_time"].max()
    surge_cutoff = latest_ts - pd.Timedelta(minutes=30)
    recent_orders_z = (
        orders[(orders["event_type"] == "ORDER_PLACED") & (orders["event_time"] >= surge_cutoff)]
        .groupby("zone_id").size().rename("recent_orders")
    )
    idle_z = (
        couriers[couriers["courier_status"] == "ONLINE_IDLE"]
        .groupby("zone_id")["courier_id"].nunique()
        .rename("idle_couriers")
    )
    surge = pd.concat([recent_orders_z, idle_z], axis=1).fillna(0).reset_index()
    surge["surge_score"] = surge["recent_orders"] / surge["idle_couriers"].replace(0, 0.5)
    surge["status"] = pd.cut(surge["surge_score"], bins=[-1, 2, 4, float("inf")],
                              labels=["Normal", "Warning", "Critical"])
    color_map  = {"Normal": "#10B981", "Warning": "#F59E0B", "Critical": "#EF4444"}
    surge["color"] = surge["status"].map(color_map)

    fig = go.Figure()
    for _, row in surge.iterrows():
        fig.add_bar(x=[row["zone_id"]], y=[row["surge_score"]],
                    marker_color=row["color"], marker_line_width=0,
                    name=str(row["status"]), showlegend=False)
    fig.add_hline(y=2, line_dash="dot",  line_color="#F59E0B",
                  annotation_text="Warning (2×)", annotation_font_color="#F59E0B",
                  annotation_position="top left")
    fig.add_hline(y=4, line_dash="dash", line_color="#EF4444",
                  annotation_text="Critical (4×)", annotation_font_color="#EF4444",
                  annotation_position="top left")
    fig.update_layout(yaxis_title="surge score", height=300, **CHART_LAYOUT)
    st.plotly_chart(fig, use_container_width=True)

    surge_cols = st.columns(len(surge))
    for i, (_, row) in enumerate(surge.iterrows()):
        icon = "🔴" if row["status"] == "Critical" else "🟡" if row["status"] == "Warning" else "🟢"
        surge_cols[i].metric(
            label=f"{icon} {row['zone_id'].capitalize()}",
            value=f"{row['surge_score']:.1f}×",
            help=f"{int(row['recent_orders'])} orders · {int(row['idle_couriers'])} idle couriers",
        )
    hotspot = surge.loc[surge["surge_score"].idxmax()]
    insight(
        f"{hotspot['zone_id'].capitalize()} is the most pressured zone ({hotspot['surge_score']:.1f}× score) — "
        f"{int(hotspot['recent_orders'])} orders against only {int(hotspot['idle_couriers'])} idle couriers. "
        f"Deploying more couriers here would reduce wait times.",
        color="#92400E" if hotspot["surge_score"] > 2 else "#6B7280",
    )
else:
    st.info("Insufficient data for surge analysis.")

# Mid-delivery courier offline events
if not couriers.empty:
    mid_offline = couriers[
        (couriers["courier_status"] == "OFFLINE") &
        (couriers["order_id"].notna()) &
        (couriers["order_id"] != "")
    ]
    if not mid_offline.empty:
        st.markdown("<p style='font-size:0.8rem;font-weight:600;color:#374151;margin:20px 0 8px;'>Couriers Going Offline Mid-Delivery</p>", unsafe_allow_html=True)
        st.dataframe(
            mid_offline[["event_time", "courier_id", "order_id", "zone_id", "battery_pct", "network_type"]]
            .sort_values("event_time", ascending=False).head(20),
            use_container_width=True,
        )

st.divider()


# ─────────────────────────────────────────────────────────────────────────────
# DATA QUALITY PANEL
# ─────────────────────────────────────────────────────────────────────────────
section_header("Data Quality", "Pipeline health · duplicate rates, late arrivals, total ingested events")

dq1, dq2, dq3, dq4 = st.columns(4)

if not orders_raw.empty and "is_duplicate" in orders_raw.columns:
    dq1.metric("Order Duplicate Rate", f"{orders_raw['is_duplicate'].sum() / max(len(orders_raw),1) * 100:.1f}%")
else:
    dq1.metric("Order Duplicate Rate", "N/A")

if not couriers_raw.empty and "is_duplicate" in couriers_raw.columns:
    dq2.metric("Courier Duplicate Rate", f"{couriers_raw['is_duplicate'].sum() / max(len(couriers_raw),1) * 100:.1f}%")
else:
    dq2.metric("Courier Duplicate Rate", "N/A")

lag_col = next((c for c in ["processing_time", "ingestion_time"] if c in orders.columns), None)
if not orders.empty and lag_col:
    lag = (pd.to_datetime(orders[lag_col], unit="ms", utc=True) - orders["event_time"]).dt.total_seconds()
    late_pct = (lag > 60).sum() / max(len(lag), 1) * 100
    dq3.metric("Late Arrivals (>60s)", f"{late_pct:.1f}%",
               help="% of order events where ingestion lag exceeded 60s within the selected time window")
else:
    dq3.metric("Late Arrivals (>60s)", "N/A")

dq4.metric("Total Raw Order Events", f"{len(orders_raw):,}" if not orders_raw.empty else "0")

st.markdown("""
<div style="margin-top:20px; background:white; border-radius:12px; padding:18px 24px;
            box-shadow:0 1px 3px rgba(0,0,0,0.06); border-left:4px solid #3B82F6;">
    <p style="margin:0 0 10px; font-size:0.72rem; font-weight:700; color:#0F172A;
              text-transform:uppercase; letter-spacing:0.08em;">How to read these metrics</p>
    <div style="display:grid; grid-template-columns:repeat(auto-fit,minmax(220px,1fr)); gap:10px;">
        <div>
            <p style="margin:0; font-size:0.78rem; font-weight:600; color:#374151;">
                Order / Courier Duplicate Rate
            </p>
            <p style="margin:3px 0 0; font-size:0.73rem; color:#6B7280; line-height:1.5;">
                Share of raw events flagged as duplicates by the Spark deduplication step.
                <strong style="color:#374151;">Target: &lt; 1%.</strong>
                Higher values mean the same event was processed more than once and skew KPI counts.
            </p>
        </div>
        <div>
            <p style="margin:0; font-size:0.78rem; font-weight:600; color:#374151;">
                Late Arrivals (&gt; 60 s)
            </p>
            <p style="margin:3px 0 0; font-size:0.73rem; color:#6B7280; line-height:1.5;">
                % of order events that arrived at the ingestion layer more than 60 seconds after
                the event timestamp. Measures end-to-end pipeline latency.
                <strong style="color:#374151;">Target: &lt; 5%.</strong>
                High values indicate network delays, Event Hub backpressure, or slow Spark checkpointing.
            </p>
        </div>
        <div>
            <p style="margin:0; font-size:0.78rem; font-weight:600; color:#374151;">
                Total Raw Order Events
            </p>
            <p style="margin:3px 0 0; font-size:0.73rem; color:#6B7280; line-height:1.5;">
                Cumulative count of all order events ever written to Azure Blob Storage,
                regardless of filters. A useful sanity check — a sudden drop signals
                that the upstream Spark job has stopped writing output.
            </p>
        </div>
    </div>
</div>
""", unsafe_allow_html=True)

st.markdown(f"""
<p style="text-align:center; color:#94A3B8; font-size:0.72rem; margin-top:24px;">
    Food Delivery Analytics · Azure Blob Storage · <code>{CONTAINER}</code> ·
    {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC
</p>
""", unsafe_allow_html=True)
