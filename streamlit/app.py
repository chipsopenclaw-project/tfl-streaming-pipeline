"""
TfL Streaming Pipeline - Streamlit Dashboard
Real-time London Underground arrivals analytics
"""

import streamlit as st
import pandas as pd
import plotly.express as px
from pyathena import connect
from datetime import date

# ── Page Config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="TfL Live Dashboard",
    page_icon="🚇",
    layout="wide",
)

# ── Athena Connection ─────────────────────────────────────────────────────────
@st.cache_resource
def get_connection():
    return connect(
        aws_access_key_id=st.secrets["aws"]["aws_access_key_id"],
        aws_secret_access_key=st.secrets["aws"]["aws_secret_access_key"],
        s3_staging_dir="s3://tfl-dev-euw2-s3-athena-results/queries/",
        region_name=st.secrets["aws"]["aws_region"],
        work_group="tfl-dev-workgroup",
    )

@st.cache_data(ttl=60)
def run_query(sql: str) -> pd.DataFrame:
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(sql)
    columns = [desc[0] for desc in cursor.description]
    rows = cursor.fetchall()
    return pd.DataFrame(rows, columns=columns)

TODAY = str(date.today())

# ── Sidebar ───────────────────────────────────────────────────────────────────
st.sidebar.title("🚇 TfL Live Dashboard")
st.sidebar.markdown("Real-time London Underground analytics")
st.sidebar.markdown("---")
page = st.sidebar.radio(
    "Select Page",
    ["🕐 Current Wait Times", "📊 Line Performance", "🗺️ Busiest Stations"]
)
st.sidebar.markdown("---")
st.sidebar.markdown(f"**Date:** {TODAY}")
st.sidebar.markdown("**Lines:** Central, Jubilee, Northern")
if st.sidebar.button("🔄 Refresh Data"):
    st.cache_data.clear()

# ══════════════════════════════════════════════════════════════════════════════
# PAGE 1 — Current Wait Times
# ══════════════════════════════════════════════════════════════════════════════
if page == "🕐 Current Wait Times":
    st.title("🕐 Current Wait Times")
    st.markdown("Latest expected wait times per station and platform.")

    with st.spinner("Loading current wait times..."):
        df = run_query("""
            SELECT
                station_name,
                line_id,
                line_name,
                platform_name,
                towards,
                wait_minutes,
                current_location,
                expected_arrival
            FROM tfl_dev_db.gold_current_wait_times
            ORDER BY line_id, wait_minutes ASC
        """)

    if df.empty:
        st.warning("No data available yet.")
    else:
        col1, col2, col3 = st.columns(3)
        col1.metric("Total Stations", df["station_name"].nunique())
        col2.metric("Avg Wait (mins)", round(df["wait_minutes"].mean(), 1))
        col3.metric("Max Wait (mins)", round(df["wait_minutes"].max(), 1))
        st.markdown("---")

        lines = ["All"] + sorted(df["line_id"].unique().tolist())
        selected_line = st.selectbox("Filter by Line", lines)
        if selected_line != "All":
            df = df[df["line_id"] == selected_line]

        fig = px.bar(
            df.head(30),
            x="station_name",
            y="wait_minutes",
            color="line_id",
            color_discrete_map={
                "central": "#E32017",
                "jubilee": "#A0A5A9",
                "northern": "#000000",
            },
            title="Current Wait Times by Station (Top 30)",
            labels={"wait_minutes": "Wait (mins)", "station_name": "Station"},
        )
        fig.update_layout(xaxis_tickangle=-45)
        st.plotly_chart(fig, use_container_width=True)

        st.subheader("Detail")
        st.dataframe(
            df[["station_name", "line_name", "platform_name", "towards", "wait_minutes", "current_location"]]
            .rename(columns={
                "station_name": "Station",
                "line_name": "Line",
                "platform_name": "Platform",
                "towards": "Towards",
                "wait_minutes": "Wait (mins)",
                "current_location": "Current Location",
            }),
            use_container_width=True,
        )

# ══════════════════════════════════════════════════════════════════════════════
# PAGE 2 — Line Performance
# ══════════════════════════════════════════════════════════════════════════════
elif page == "📊 Line Performance":
    st.title("📊 Line Performance")
    st.markdown("Average wait times per line today.")

    with st.spinner("Loading line performance..."):
        df = run_query(f"""
            SELECT
                line_id,
                line_name,
                ROUND(avg_wait_seconds / 60, 2) AS avg_wait_minutes,
                ROUND(min_wait_seconds / 60, 2) AS min_wait_minutes,
                ROUND(max_wait_seconds / 60, 2) AS max_wait_minutes,
                record_count
            FROM tfl_dev_db.gold_line_performance
            WHERE arrival_date = '{TODAY}'
            ORDER BY avg_wait_minutes DESC
        """)

    if df.empty:
        st.warning("No data available for today yet.")
    else:
        col1, col2, col3 = st.columns(3)
        busiest = df.iloc[0]
        col1.metric("Slowest Line", busiest["line_name"], f"{busiest['avg_wait_minutes']} mins avg")
        col2.metric("Total Records", df["record_count"].sum())
        col3.metric("Lines Monitored", len(df))
        st.markdown("---")

        fig = px.bar(
            df,
            x="line_name",
            y=["min_wait_minutes", "avg_wait_minutes", "max_wait_minutes"],
            barmode="group",
            color_discrete_sequence=["#00A651", "#E32017", "#FF6B35"],
            title="Wait Times by Line (Min / Avg / Max)",
            labels={"value": "Wait (mins)", "line_name": "Line"},
        )
        st.plotly_chart(fig, use_container_width=True)

        st.subheader("Detail")
        st.dataframe(
            df.rename(columns={
                "line_id": "Line ID",
                "line_name": "Line",
                "avg_wait_minutes": "Avg Wait (mins)",
                "min_wait_minutes": "Min Wait (mins)",
                "max_wait_minutes": "Max Wait (mins)",
                "record_count": "Records",
            }),
            use_container_width=True,
        )

# ══════════════════════════════════════════════════════════════════════════════
# PAGE 3 — Busiest Stations
# ══════════════════════════════════════════════════════════════════════════════
elif page == "🗺️ Busiest Stations":
    st.title("🗺️ Busiest Stations")
    st.markdown("Arrival counts by station and hour today.")

    with st.spinner("Loading station data..."):
        df = run_query(f"""
            SELECT
                station_name,
                line_id,
                arrival_hour,
                arrival_count,
                ROUND(avg_wait_seconds / 60, 2) AS avg_wait_minutes
            FROM tfl_dev_db.gold_station_arrivals
            WHERE arrival_date = '{TODAY}'
            ORDER BY arrival_count DESC
        """)

    if df.empty:
        st.warning("No data available for today yet.")
    else:
        col1, col2, col3 = st.columns(3)
        col1.metric("Total Stations", df["station_name"].nunique())
        col2.metric("Busiest Station", df.iloc[0]["station_name"])
        col3.metric("Peak Arrivals", df.iloc[0]["arrival_count"])
        st.markdown("---")

        top20 = df.groupby("station_name")["arrival_count"].sum().reset_index()
        top20 = top20.sort_values("arrival_count", ascending=False).head(20)

        fig = px.bar(
            top20,
            x="station_name",
            y="arrival_count",
            title="Top 20 Busiest Stations Today",
            labels={"arrival_count": "Arrival Count", "station_name": "Station"},
            color="arrival_count",
            color_continuous_scale="Reds",
        )
        fig.update_layout(xaxis_tickangle=-45)
        st.plotly_chart(fig, use_container_width=True)

        st.subheader("Arrivals by Hour Heatmap")
        top10_stations = top20.head(10)["station_name"].tolist()
        heatmap_df = df[df["station_name"].isin(top10_stations)]
        pivot = heatmap_df.pivot_table(
            index="station_name",
            columns="arrival_hour",
            values="arrival_count",
            aggfunc="sum",
            fill_value=0,
        )
        fig2 = px.imshow(
            pivot,
            title="Top 10 Stations — Arrivals by Hour",
            labels={"x": "Hour", "y": "Station", "color": "Arrivals"},
            color_continuous_scale="Reds",
        )
        st.plotly_chart(fig2, use_container_width=True)

        st.subheader("Detail")
        st.dataframe(
            df.head(50).rename(columns={
                "station_name": "Station",
                "line_id": "Line",
                "arrival_hour": "Hour",
                "arrival_count": "Arrivals",
                "avg_wait_minutes": "Avg Wait (mins)",
            }),
            use_container_width=True,
        )
