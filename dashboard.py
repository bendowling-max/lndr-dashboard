"""
LNDR Brain — Purchase Orders Dashboard
Run locally:  streamlit run dashboard.py
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from google.cloud import bigquery
from datetime import date, timedelta

# ── Page config ───────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="LNDR — Purchase Orders",
    page_icon="📦",
    layout="wide",
)

# ── Styling ───────────────────────────────────────────────────────────────────

st.markdown("""
<style>
    .metric-card {
        background: #1e1e2e;
        border-radius: 12px;
        padding: 20px 24px;
        border-left: 4px solid #7c3aed;
    }
    .metric-label { color: #94a3b8; font-size: 13px; font-weight: 500; margin-bottom: 4px; }
    .metric-value { color: #f1f5f9; font-size: 28px; font-weight: 700; }
    .metric-sub   { color: #64748b; font-size: 12px; margin-top: 4px; }
    .overdue-card { border-left-color: #dc2626 !important; }
    .warning-card { border-left-color: #d97706 !important; }
    .good-card    { border-left-color: #16a34a !important; }
    h1 { color: #f1f5f9 !important; }
    .stDataFrame { font-size: 13px; }
    [data-testid="stSidebar"] { background: #0f0f1a; }
</style>
""", unsafe_allow_html=True)

# ── BigQuery connection ────────────────────────────────────────────────────────

@st.cache_resource
def get_bq_client():
    # On Streamlit Cloud: reads from st.secrets["gcp_service_account"]
    # Locally: uses Application Default Credentials
    if "gcp_service_account" in st.secrets:
        from google.oauth2 import service_account
        creds = service_account.Credentials.from_service_account_info(
            st.secrets["gcp_service_account"]
        )
        return bigquery.Client(credentials=creds, project="lndr-brain")
    return bigquery.Client(project="lndr-brain")


@st.cache_data(ttl=3600, show_spinner="Loading data from BigQuery...")
def load_pos():
    bq = get_bq_client()
    query = """
    WITH latest_fx AS (
      SELECT eur, usd FROM `lndr-brain.reference.exchange_rates`
      ORDER BY date DESC LIMIT 1
    ),
    po_gbp AS (
      SELECT
        po.po_id,
        po.reference,
        COALESCE(po.vendor, 'Unknown') AS vendor,
        po.status,
        COALESCE(po.warehouse, 'Unknown') AS warehouse,
        po.currency,
        po.expected_date,
        po.received_date,
        DATE(po.created_at)  AS created_date,
        po.total,
        po.total_ordered,
        po.total_received,
        po.total_remaining,
        po.item_count,
        po.notes,
        CASE po.currency
          WHEN 'GBP' THEN po.total
          WHEN 'EUR' THEN SAFE_DIVIDE(po.total, COALESCE(fx.eur, lfx.eur))
          WHEN 'USD' THEN SAFE_DIVIDE(po.total, COALESCE(fx.usd, lfx.usd))
        END AS total_gbp
      FROM `lndr-brain.inventory_planner_raw.purchase_orders` po
      LEFT JOIN `lndr-brain.reference.exchange_rates` fx
        ON fx.date = po.expected_date
      CROSS JOIN latest_fx lfx
      WHERE po.expected_date IS NOT NULL
    )
    SELECT * FROM po_gbp
    """
    df = bq.query(query).to_dataframe()
    df["expected_date"] = pd.to_datetime(df["expected_date"])
    df["created_date"]  = pd.to_datetime(df["created_date"])
    df["received_date"] = pd.to_datetime(df["received_date"])
    return df


@st.cache_data(ttl=3600, show_spinner=False)
def load_po_items():
    bq = get_bq_client()
    query = """
    SELECT po_id, sku, title, qty_ordered, qty_received, qty_remaining, cost_price, total_cost
    FROM `lndr-brain.inventory_planner_raw.purchase_order_items`
    """
    return bq.query(query).to_dataframe()


# ── Helpers ───────────────────────────────────────────────────────────────────

OPEN_STATUSES   = {"open", "open (uploaded)", "partially received"}
CLOSED_STATUSES = {"closed", "closed (uploaded)"}

def is_open(status):
    return str(status).lower() in OPEN_STATUSES

def fmt_gbp(val):
    if pd.isna(val) or val is None:
        return "—"
    if abs(val) >= 1_000_000:
        return f"£{val/1_000_000:.1f}m"
    if abs(val) >= 1_000:
        return f"£{val/1_000:.0f}k"
    return f"£{val:,.0f}"

def fmt_int(val):
    if pd.isna(val):
        return "—"
    return f"{int(val):,}"


# ── Load data ─────────────────────────────────────────────────────────────────

df_all  = load_pos()
df_items = load_po_items()
today   = pd.Timestamp(date.today())

# ── Sidebar filters ───────────────────────────────────────────────────────────

with st.sidebar:
    st.image("https://cdn.shopify.com/s/files/1/0263/9750/7672/t/8/assets/lndr_logo_white.png",
             width=120, use_column_width=False)
    st.markdown("---")
    st.markdown("### Filters")

    all_statuses = sorted(df_all["status"].dropna().unique())
    sel_statuses = st.multiselect(
        "Status",
        options=all_statuses,
        default=[s for s in all_statuses if s in OPEN_STATUSES],
    )

    all_vendors = sorted(df_all["vendor"].dropna().unique())
    sel_vendors = st.multiselect("Vendor", options=all_vendors, default=[])

    all_warehouses = sorted(df_all["warehouse"].dropna().unique())
    sel_warehouses = st.multiselect("Warehouse", options=all_warehouses, default=[])

    min_date = df_all["expected_date"].min().date()
    max_date = df_all["expected_date"].max().date()
    date_range = st.date_input(
        "Expected delivery range",
        value=(date.today() - timedelta(days=365), max_date),
        min_value=min_date,
        max_value=max_date,
    )

    st.markdown("---")
    if st.button("🔄 Refresh data"):
        st.cache_data.clear()
        st.rerun()

    st.caption(f"Data cached 1h · Last load: {pd.Timestamp.now().strftime('%H:%M')}")

# ── Apply filters ─────────────────────────────────────────────────────────────

df = df_all.copy()
if sel_statuses:
    df = df[df["status"].isin(sel_statuses)]
if sel_vendors:
    df = df[df["vendor"].isin(sel_vendors)]
if sel_warehouses:
    df = df[df["warehouse"].isin(sel_warehouses)]
if len(date_range) == 2:
    df = df[
        (df["expected_date"] >= pd.Timestamp(date_range[0])) &
        (df["expected_date"] <= pd.Timestamp(date_range[1]))
    ]

# Separate open vs all for KPIs
df_open = df_all[df_all["status"].isin(OPEN_STATUSES)]

# ── Header ────────────────────────────────────────────────────────────────────

st.markdown("# 📦 LNDR Purchase Orders")
st.caption(f"Showing **{len(df):,}** purchase orders · Data from Inventory Planner · Updates nightly")

# ── KPI Cards ─────────────────────────────────────────────────────────────────

st.markdown("---")
k1, k2, k3, k4, k5 = st.columns(5)

open_value   = df_open["total_gbp"].sum()
open_units   = df_open["total_remaining"].sum()
open_count   = len(df_open)

due_this_month = df_open[
    (df_open["expected_date"].dt.year == today.year) &
    (df_open["expected_date"].dt.month == today.month)
]
due_next_30    = df_open[
    (df_open["expected_date"] >= today) &
    (df_open["expected_date"] <= today + timedelta(days=30))
]
overdue = df_open[df_open["expected_date"] < today]

with k1:
    st.markdown(f"""
    <div class="metric-card good-card">
        <div class="metric-label">OPEN PO VALUE</div>
        <div class="metric-value">{fmt_gbp(open_value)}</div>
        <div class="metric-sub">{open_count:,} open purchase orders</div>
    </div>""", unsafe_allow_html=True)

with k2:
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-label">UNITS PENDING</div>
        <div class="metric-value">{fmt_int(open_units)}</div>
        <div class="metric-sub">units not yet received</div>
    </div>""", unsafe_allow_html=True)

with k3:
    st.markdown(f"""
    <div class="metric-card warning-card">
        <div class="metric-label">DUE THIS MONTH</div>
        <div class="metric-value">{fmt_gbp(due_this_month['total_gbp'].sum())}</div>
        <div class="metric-sub">{len(due_this_month):,} POs expected</div>
    </div>""", unsafe_allow_html=True)

with k4:
    st.markdown(f"""
    <div class="metric-card warning-card">
        <div class="metric-label">DUE NEXT 30 DAYS</div>
        <div class="metric-value">{fmt_gbp(due_next_30['total_gbp'].sum())}</div>
        <div class="metric-sub">{len(due_next_30):,} POs arriving soon</div>
    </div>""", unsafe_allow_html=True)

with k5:
    card_class = "overdue-card" if len(overdue) > 0 else "good-card"
    st.markdown(f"""
    <div class="metric-card {card_class}">
        <div class="metric-label">OVERDUE</div>
        <div class="metric-value">{fmt_gbp(overdue['total_gbp'].sum())}</div>
        <div class="metric-sub">{len(overdue):,} POs past expected date</div>
    </div>""", unsafe_allow_html=True)

st.markdown("<br>", unsafe_allow_html=True)

# ── Delivery Timeline ─────────────────────────────────────────────────────────

st.markdown("### 📅 Delivery Timeline")

monthly = (
    df.assign(month=df["expected_date"].dt.to_period("M").dt.to_timestamp())
    .groupby(["month", "status"])
    .agg(total_gbp=("total_gbp", "sum"), po_count=("po_id", "count"), units=("total_ordered", "sum"))
    .reset_index()
)
monthly["month_label"] = monthly["month"].dt.strftime("%b %Y")

fig_timeline = px.bar(
    monthly,
    x="month_label",
    y="total_gbp",
    color="status",
    color_discrete_map={
        "open":                "#7c3aed",
        "open (uploaded)":     "#a855f7",
        "partially received":  "#f59e0b",
        "closed":              "#374151",
        "closed (uploaded)":   "#1f2937",
        "canceled":            "#7f1d1d",
    },
    labels={"total_gbp": "Value (GBP)", "month_label": "Expected Delivery Month", "status": "Status"},
    hover_data={"po_count": True, "units": True},
    template="plotly_dark",
)
fig_timeline.update_layout(
    height=380,
    margin=dict(l=0, r=0, t=20, b=0),
    legend=dict(orientation="h", y=-0.15),
    yaxis_tickprefix="£",
    yaxis_tickformat=",.0f",
    plot_bgcolor="#0f0f1a",
    paper_bgcolor="#0f0f1a",
    xaxis=dict(tickangle=-45),
)
fig_timeline.add_vline(
    x=today.strftime("%b %Y"),
    line_dash="dash",
    line_color="#94a3b8",
    annotation_text="Today",
    annotation_position="top",
)
st.plotly_chart(fig_timeline, use_container_width=True)

# ── Vendor & Warehouse breakdowns ────────────────────────────────────────────

col_v, col_w = st.columns(2)

with col_v:
    st.markdown("### 🏭 By Vendor")
    vendor_df = (
        df[df["status"].isin(OPEN_STATUSES)]
        .groupby("vendor")
        .agg(total_gbp=("total_gbp", "sum"), po_count=("po_id", "count"), units=("total_remaining", "sum"))
        .reset_index()
        .sort_values("total_gbp", ascending=True)
        .tail(12)
    )
    fig_vendor = px.bar(
        vendor_df,
        x="total_gbp",
        y="vendor",
        orientation="h",
        color="total_gbp",
        color_continuous_scale="Purples",
        labels={"total_gbp": "Open Value (GBP)", "vendor": ""},
        template="plotly_dark",
        hover_data={"po_count": True, "units": True},
    )
    fig_vendor.update_layout(
        height=380,
        margin=dict(l=0, r=0, t=10, b=0),
        showlegend=False,
        coloraxis_showscale=False,
        xaxis_tickprefix="£",
        xaxis_tickformat=",.0f",
        plot_bgcolor="#0f0f1a",
        paper_bgcolor="#0f0f1a",
    )
    st.plotly_chart(fig_vendor, use_container_width=True)

with col_w:
    st.markdown("### 🏬 By Warehouse")
    wh_df = (
        df[df["status"].isin(OPEN_STATUSES)]
        .groupby("warehouse")
        .agg(total_gbp=("total_gbp", "sum"), po_count=("po_id", "count"), units=("total_remaining", "sum"))
        .reset_index()
        .sort_values("total_gbp", ascending=False)
    )
    fig_wh = px.pie(
        wh_df,
        values="total_gbp",
        names="warehouse",
        hole=0.5,
        color_discrete_sequence=px.colors.sequential.Purples_r,
        template="plotly_dark",
    )
    fig_wh.update_traces(textposition="outside", textinfo="label+percent")
    fig_wh.update_layout(
        height=380,
        margin=dict(l=20, r=20, t=10, b=40),
        showlegend=False,
        plot_bgcolor="#0f0f1a",
        paper_bgcolor="#0f0f1a",
    )
    st.plotly_chart(fig_wh, use_container_width=True)

# ── Annual summary ────────────────────────────────────────────────────────────

st.markdown("### 📊 Annual Summary")

annual = (
    df.assign(year=df["expected_date"].dt.year.astype(str))
    .groupby(["year", "status"])
    .agg(total_gbp=("total_gbp", "sum"), po_count=("po_id", "count"), units=("total_ordered", "sum"))
    .reset_index()
)
fig_annual = px.bar(
    annual,
    x="year",
    y="total_gbp",
    color="status",
    color_discrete_map={
        "open":               "#7c3aed",
        "open (uploaded)":    "#a855f7",
        "partially received": "#f59e0b",
        "closed":             "#374151",
        "closed (uploaded)":  "#1f2937",
        "canceled":           "#7f1d1d",
    },
    barmode="stack",
    labels={"total_gbp": "Value (GBP)", "year": "Year", "status": "Status"},
    template="plotly_dark",
    hover_data={"po_count": True, "units": True},
)
fig_annual.update_layout(
    height=320,
    margin=dict(l=0, r=0, t=10, b=0),
    legend=dict(orientation="h", y=-0.2),
    yaxis_tickprefix="£",
    yaxis_tickformat=",.0f",
    plot_bgcolor="#0f0f1a",
    paper_bgcolor="#0f0f1a",
)
st.plotly_chart(fig_annual, use_container_width=True)

# ── Upcoming & Overdue ────────────────────────────────────────────────────────

tab_upcoming, tab_overdue, tab_all = st.tabs([
    f"🚚 Arriving Next 30 Days ({len(due_next_30)})",
    f"⚠️ Overdue ({len(overdue)})",
    f"📋 All POs ({len(df):,})",
])

def po_display_df(d):
    out = d[[
        "reference", "vendor", "warehouse", "status",
        "expected_date", "total_gbp", "total_ordered",
        "total_received", "total_remaining", "currency", "notes"
    ]].copy()
    out["expected_date"] = out["expected_date"].dt.strftime("%d %b %Y")
    out["total_gbp"]     = out["total_gbp"].apply(lambda x: f"£{x:,.0f}" if pd.notna(x) else "—")
    out["total_ordered"] = out["total_ordered"].apply(lambda x: f"{int(x):,}" if pd.notna(x) else "—")
    out["total_received"]= out["total_received"].apply(lambda x: f"{int(x):,}" if pd.notna(x) else "—")
    out["total_remaining"]=out["total_remaining"].apply(lambda x: f"{int(x):,}" if pd.notna(x) else "—")
    out.columns = ["Reference", "Vendor", "Warehouse", "Status",
                   "Expected", "Value (GBP)", "Ordered", "Received", "Remaining", "Currency", "Notes"]
    return out

with tab_upcoming:
    if len(due_next_30) == 0:
        st.info("No open POs expected in the next 30 days.")
    else:
        st.dataframe(
            po_display_df(due_next_30.sort_values("expected_date")),
            use_container_width=True, hide_index=True,
        )

with tab_overdue:
    if len(overdue) == 0:
        st.success("No overdue POs.")
    else:
        st.warning(f"{len(overdue)} open POs are past their expected delivery date.")
        st.dataframe(
            po_display_df(overdue.sort_values("expected_date")),
            use_container_width=True, hide_index=True,
        )

with tab_all:
    search = st.text_input("🔍 Search by reference, vendor, or SKU", placeholder="e.g. LTP, PO-123")
    df_show = df.copy()
    if search:
        mask = (
            df_show["reference"].str.contains(search, case=False, na=False) |
            df_show["vendor"].str.contains(search, case=False, na=False)
        )
        df_show = df_show[mask]
    st.dataframe(
        po_display_df(df_show.sort_values("expected_date", ascending=False)),
        use_container_width=True, hide_index=True, height=500,
    )

# ── PO Items drilldown ────────────────────────────────────────────────────────

st.markdown("---")
st.markdown("### 🔎 Drill into a PO")

po_refs = sorted(df[df["status"].isin(OPEN_STATUSES)]["reference"].dropna().unique())
selected_ref = st.selectbox("Select a purchase order", options=[""] + list(po_refs))

if selected_ref:
    po_row = df[df["reference"] == selected_ref].iloc[0]
    items  = df_items[df_items["po_id"] == po_row["po_id"]]

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Vendor",    po_row["vendor"])
    c2.metric("Warehouse", po_row["warehouse"])
    c3.metric("Expected",  po_row["expected_date"].strftime("%d %b %Y") if pd.notna(po_row["expected_date"]) else "—")
    c4.metric("Value",     fmt_gbp(po_row["total_gbp"]))

    if len(items) > 0:
        items_show = items[["sku", "title", "qty_ordered", "qty_received", "qty_remaining", "cost_price", "total_cost"]].copy()
        items_show.columns = ["SKU", "Product", "Ordered", "Received", "Remaining", "Cost Price", "Total Cost"]
        st.dataframe(items_show, use_container_width=True, hide_index=True)
    else:
        st.info("No line item detail available for this PO.")
