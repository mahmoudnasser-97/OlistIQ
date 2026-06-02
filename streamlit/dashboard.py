import time
import redis
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

# ============================================================
# PAGE CONFIGURATION
# ============================================================

st.set_page_config(
    page_title="OlistIQ — Live Operations Dashboard",
    page_icon="🛒",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# ============================================================
# REDIS CONNECTION
# Streamlit runs inside Docker so we use the service name
# ============================================================

@st.cache_resource
def get_redis_connection():
    return redis.Redis(host="redis", port=6379, decode_responses=True)

r = get_redis_connection()

# ============================================================
# DATA FETCHING FUNCTIONS
# Each function reads a specific slice of data from Redis
# ============================================================

def fetch_metrics():
    """Fetch top-level scalar metrics."""
    total_orders     = int(r.get("metrics:total_orders") or 0)
    total_revenue    = float(r.get("metrics:total_revenue") or 0)
    total_freight    = float(r.get("metrics:total_freight") or 0)
    score_sum        = float(r.get("metrics:review_score_sum") or 0)
    score_count      = int(r.get("metrics:review_score_count") or 1)
    avg_score        = round(score_sum / score_count, 2)
    avg_order_value  = round(total_revenue / max(total_orders, 1), 2)
    return {
        "total_orders":    total_orders,
        "total_revenue":   round(total_revenue, 2),
        "total_freight":   round(total_freight, 2),
        "avg_score":       avg_score,
        "avg_order_value": avg_order_value
    }


def fetch_counter(prefix):
    """
    Fetch all keys under a counter prefix and return as a DataFrame.
    Example: prefix='counters:status' returns all order status counts.
    """
    keys = r.keys(f"{prefix}:*")
    if not keys:
        return pd.DataFrame(columns=["label", "count"])
    values = r.mget(keys)
    labels = [k.replace(f"{prefix}:", "") for k in keys]
    counts = [int(v) for v in values]
    df = pd.DataFrame({"label": labels, "count": counts})
    return df.sort_values("count", ascending=False).reset_index(drop=True)


def fetch_recent_events(n=12):
    """
    Fetch the n most recent order events from Redis.
    Each event is stored as a hash under key event:{order_id}.
    """
    order_ids = r.lrange("recent_events", 0, n - 1)
    events = []
    for oid in order_ids:
        data = r.hgetall(f"event:{oid}")
        if data:
            events.append(data)
    if not events:
        return pd.DataFrame()
    df = pd.DataFrame(events)
    # Cast numeric columns
    for col in ["payment_value", "price", "freight_value", "review_score"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


# ============================================================
# STYLING HELPERS
# ============================================================

COLORS = {
    "green":  "#4CAF50",
    "blue":   "#2196F3",
    "orange": "#FF9800",
    "red":    "#F44336",
    "purple": "#9C27B0",
    "teal":   "#009688",
    "chart_sequence": [
        "#4CAF50", "#2196F3", "#FF9800", "#F44336",
        "#9C27B0", "#009688", "#FF5722", "#607D8B"
    ]
}


def metric_card(label, value, suffix=""):
    st.markdown(
        f"""
        <div style="
            background: #1e1e2e;
            border-radius: 12px;
            padding: 20px 24px;
            border-left: 4px solid {COLORS['green']};
            margin-bottom: 8px;
        ">
            <div style="color:#aaa; font-size:13px; margin-bottom:6px;">{label}</div>
            <div style="color:#fff; font-size:28px; font-weight:700;">
                {value}<span style="font-size:16px; color:#aaa;"> {suffix}</span>
            </div>
        </div>
        """,
        unsafe_allow_html=True
    )


# ============================================================
# DASHBOARD HEADER
# ============================================================

st.markdown(
    """
    <div style="text-align:center; padding: 10px 0 24px 0;">
        <h1 style="color:#4CAF50; font-size:36px; margin:0;">
            🛒 OlistIQ — Live Operations Dashboard
        </h1>
        <p style="color:#aaa; margin:6px 0 0 0;">
            Real-time order stream · Kafka → Spark Streaming → Redis → Streamlit
        </p>
    </div>
    """,
    unsafe_allow_html=True
)

# ============================================================
# AUTO-REFRESH LOOP
# Streamlit reruns the entire script every N seconds.
# This is what makes the dashboard "live".
# ============================================================

REFRESH_SECONDS = 5

# Placeholders let us update specific sections without
# re-rendering the entire page, which prevents flickering.
placeholder = st.empty()

while True:

    metrics      = fetch_metrics()
    status_df    = fetch_counter("counters:status")
    payment_df   = fetch_counter("counters:payment")
    category_df  = fetch_counter("counters:category")
    state_df     = fetch_counter("counters:state")
    recent_df    = fetch_recent_events(12)

    with placeholder.container():

        # -------------------------------------------------------
        # ROW 1 — KPI METRIC CARDS
        # -------------------------------------------------------
        st.markdown("### 📊 Key Metrics")
        c1, c2, c3, c4, c5 = st.columns(5)

        with c1:
            metric_card("Total Orders", f"{metrics['total_orders']:,}")
        with c2:
            metric_card("Total Revenue", f"R$ {metrics['total_revenue']:,.2f}")
        with c3:
            metric_card("Avg Order Value", f"R$ {metrics['avg_order_value']:,.2f}")
        with c4:
            metric_card("Avg Review Score", f"{metrics['avg_score']}", suffix="/ 5")
        with c5:
            metric_card("Total Freight", f"R$ {metrics['total_freight']:,.2f}")

        st.markdown("---")

        # -------------------------------------------------------
        # ROW 2 — ORDER STATUS + PAYMENT TYPE
        # -------------------------------------------------------
        col_left, col_right = st.columns(2)

        with col_left:
            st.markdown("#### 📦 Orders by Status")
            if not status_df.empty:
                fig = px.bar(
                    status_df,
                    x="count", y="label",
                    orientation="h",
                    color="label",
                    color_discrete_sequence=COLORS["chart_sequence"],
                    text="count"
                )
                fig.update_layout(
                    paper_bgcolor="rgba(0,0,0,0)",
                    plot_bgcolor="rgba(0,0,0,0)",
                    showlegend=False,
                    margin=dict(l=0, r=0, t=10, b=0),
                    yaxis_title="",
                    xaxis_title="Orders"
                )
                fig.update_traces(textposition="outside")
                st.plotly_chart(fig, use_container_width=True)

        with col_right:
            st.markdown("#### 💳 Payment Type Distribution")
            if not payment_df.empty:
                fig = px.pie(
                    payment_df,
                    names="label",
                    values="count",
                    color_discrete_sequence=COLORS["chart_sequence"],
                    hole=0.45
                )
                fig.update_layout(
                    paper_bgcolor="rgba(0,0,0,0)",
                    plot_bgcolor="rgba(0,0,0,0)",
                    margin=dict(l=0, r=0, t=10, b=0),
                    legend=dict(font=dict(color="white"))
                )
                st.plotly_chart(fig, use_container_width=True)

        st.markdown("---")

        # -------------------------------------------------------
        # ROW 3 — TOP CATEGORIES + TOP STATES
        # -------------------------------------------------------
        col_left2, col_right2 = st.columns(2)

        with col_left2:
            st.markdown("#### 🏷️ Top 10 Product Categories")
            if not category_df.empty:
                top_cat = category_df.head(10)
                fig = px.bar(
                    top_cat,
                    x="label", y="count",
                    color="count",
                    color_continuous_scale="Greens",
                    text="count"
                )
                fig.update_layout(
                    paper_bgcolor="rgba(0,0,0,0)",
                    plot_bgcolor="rgba(0,0,0,0)",
                    showlegend=False,
                    margin=dict(l=0, r=0, t=10, b=0),
                    xaxis_title="",
                    yaxis_title="Orders",
                    coloraxis_showscale=False
                )
                fig.update_xaxes(tickangle=45)
                fig.update_traces(textposition="outside")
                st.plotly_chart(fig, use_container_width=True)

        with col_right2:
            st.markdown("#### 🗺️ Orders by Brazilian State")
            if not state_df.empty:
                fig = px.choropleth(
                    state_df,
                    locations="label",
                    color="count",
                    locationmode="USA-states",
                    scope="south america",
                    color_continuous_scale="Greens",
                    hover_name="label",
                    hover_data={"count": True}
                )
                fig.update_layout(
                    paper_bgcolor="rgba(0,0,0,0)",
                    plot_bgcolor="rgba(0,0,0,0)",
                    geo=dict(bgcolor="rgba(0,0,0,0)"),
                    margin=dict(l=0, r=0, t=10, b=0),
                    coloraxis_showscale=False
                )
                st.plotly_chart(fig, use_container_width=True)

        st.markdown("---")

        # -------------------------------------------------------
        # ROW 4 — LIVE EVENT FEED TABLE
        # -------------------------------------------------------
        st.markdown("#### ⚡ Live Event Feed — Last 12 Orders")
        if not recent_df.empty:
            display_cols = [
                "order_id", "order_status", "customer_state",
                "product_category", "payment_type",
                "payment_value", "review_score", "event_timestamp"
            ]
            available = [c for c in display_cols if c in recent_df.columns]
            display_df = recent_df[available].copy()

            if "order_id" in display_df.columns:
                display_df["order_id"] = display_df["order_id"].str[:8] + "..."
            if "event_timestamp" in display_df.columns:
                display_df["event_timestamp"] = pd.to_datetime(
                    display_df["event_timestamp"], errors="coerce"
                ).dt.strftime("%H:%M:%S")

            display_df.columns = [
                c.replace("_", " ").title() for c in display_df.columns
            ]
            st.dataframe(
                display_df,
                use_container_width=True,
                hide_index=True
            )
        else:
            st.info("Waiting for events from the stream...")

        # -------------------------------------------------------
        # FOOTER — refresh countdown
        # -------------------------------------------------------
        st.markdown(
            f"<p style='text-align:center; color:#555; font-size:12px;'>"
            f"🔄 Refreshing every {REFRESH_SECONDS} seconds</p>",
            unsafe_allow_html=True
        )

    time.sleep(REFRESH_SECONDS)