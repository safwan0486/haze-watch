"""Haze Watch dashboard (Streamlit). Reads the marts in read-only mode.

Run:  streamlit run dashboard/app.py
"""

from __future__ import annotations

import altair as alt
import duckdb
import pandas as pd
import streamlit as st

from hazewatch.config import get_settings

WHO_PM25_24H = 15.0  # WHO 2021 guideline, ug/m3 (24-hour mean)
# Official US EPA AQI categories and colours, in order of severity.
AQI_CATEGORIES = ["Good", "Moderate", "Unhealthy for Sensitive Groups", "Unhealthy", "Very Unhealthy", "Hazardous"]
AQI_COLORS = ["#00e400", "#ffff00", "#ff7e00", "#ff0000", "#8f3f97", "#7e0023"]

st.set_page_config(page_title="Haze Watch", page_icon="🌫️", layout="wide")
settings = get_settings()


@st.cache_data(ttl=300)
def query(sql: str) -> pd.DataFrame:
    with duckdb.connect(str(settings.warehouse_path), read_only=True) as con:
        return con.execute(sql).df()


st.title("🌫️ Haze Watch — Southeast Asia air quality")
st.caption("Hourly PM2.5 and US AQI for six cities · Source: Open-Meteo (CAMS model data) · "
           "Pipeline: API → bronze JSON → silver Parquet → DuckDB star schema")

if not settings.warehouse_path.exists():
    st.warning("No warehouse found yet. Run `hazewatch run --past-days 7` first.")
    st.stop()

try:
    daily = query("SELECT * FROM marts.mart_city_daily")
    hourly = query("""SELECT f.observed_at_local, c.city_name, f.pm2_5, f.us_aqi, f.aqi_category
                      FROM marts.fct_air_quality_hourly f JOIN marts.dim_city c USING (city_id)""")
    episodes = query("SELECT * FROM marts.mart_haze_episodes")
    last_run = query("""SELECT run_id, max(finished_at) AS finished_at,
                               bool_and(status = 'success') AS ok
                        FROM ops.pipeline_runs GROUP BY run_id ORDER BY finished_at DESC LIMIT 1""")
    dq = query("""SELECT check_name, stage, severity, passed, failing_rows FROM ops.dq_results
                  WHERE run_id = (SELECT run_id FROM ops.dq_results ORDER BY checked_at DESC LIMIT 1)
                  ORDER BY stage DESC, check_name""")
except duckdb.Error as exc:  # e.g. the pipeline is writing right now
    st.error(f"Warehouse is busy or not built yet: {exc}")
    st.stop()

cities = sorted(daily["city_name"].unique())
selected = st.multiselect("Cities", cities, default=cities)
daily = daily[daily["city_name"].isin(selected)]
hourly = hourly[hourly["city_name"].isin(selected)]
full_days = daily[daily["pm2_5_completeness"] >= 0.75]

# --- KPI row --------------------------------------------------------------------------
c1, c2, c3, c4 = st.columns(4)
worst = full_days.groupby("city_name")["avg_pm2_5"].mean().sort_values(ascending=False)
if len(worst):
    c1.metric(f"Most polluted · {worst.index[0]}", f"{worst.iloc[0]:.1f} µg/m³",
              help="Mean of daily average PM2.5 over complete days")
c2.metric("City-days above WHO PM2.5 guideline",
          f"{int(full_days['exceeds_who_pm2_5_24h'].sum())} / {len(full_days)}",
          help=f"Daily mean PM2.5 above {WHO_PM25_24H:g} µg/m³ (WHO 2021)")
c3.metric("Unhealthy hours (AQI > 150)", f"{int(daily['hours_unhealthy'].sum()):,}")
if len(last_run):
    status = "✅ Healthy" if bool(last_run["ok"].iloc[0]) else "❌ Failed"
    c4.metric(f"Last run · {str(last_run['finished_at'].iloc[0])[:16]} UTC", status)

# --- Charts ---------------------------------------------------------------------------
st.subheader("Hourly PM2.5 (µg/m³, local time)")
line = (
    alt.Chart(hourly)
    .mark_line(strokeWidth=1.6)
    .encode(
        x=alt.X("observed_at_local:T", title=None),
        y=alt.Y("pm2_5:Q", title="PM2.5 (µg/m³)"),
        color=alt.Color("city_name:N", title="City"),
        tooltip=["city_name", alt.Tooltip("observed_at_local:T", format="%d %b %H:%M"), "pm2_5", "us_aqi"],
    )
)
who_rule = alt.Chart(pd.DataFrame({"y": [WHO_PM25_24H]})).mark_rule(strokeDash=[4, 4], color="gray").encode(y="y:Q")
st.altair_chart((line + who_rule).properties(height=320).interactive(bind_y=False), use_container_width=True)

left, right = st.columns(2)
with left:
    st.subheader("Average daily PM2.5 vs WHO guideline")
    avg = full_days.groupby("city_name", as_index=False)["avg_pm2_5"].mean()
    avg["above_who"] = avg["avg_pm2_5"] > WHO_PM25_24H
    bars = (
        alt.Chart(avg)
        .mark_bar()
        .encode(
            x=alt.X("avg_pm2_5:Q", title="Average daily PM2.5 (µg/m³)"),
            y=alt.Y("city_name:N", sort="-x", title=None),
            color=alt.Color("above_who:N", title="Above WHO",
                            scale=alt.Scale(domain=[False, True], range=["#4c9be8", "#e8604c"])),
            tooltip=["city_name", alt.Tooltip("avg_pm2_5:Q", format=".1f")],
        )
    )
    rule = alt.Chart(pd.DataFrame({"x": [WHO_PM25_24H]})).mark_rule(strokeDash=[4, 4], color="gray").encode(x="x:Q")
    st.altair_chart((bars + rule).properties(height=300), use_container_width=True)
    st.caption(f"Dashed line = WHO 24-hour guideline ({WHO_PM25_24H:g} µg/m³).")
with right:
    st.subheader("Share of hours by AQI category")
    share = hourly.dropna(subset=["aqi_category"]).groupby(["city_name", "aqi_category"], as_index=False).size()
    stacked = (
        alt.Chart(share)
        .mark_bar()
        .encode(
            x=alt.X("size:Q", stack="normalize", title="Share of hours", axis=alt.Axis(format="%")),
            y=alt.Y("city_name:N", title=None),
            color=alt.Color("aqi_category:N", title="US AQI",
                            scale=alt.Scale(domain=AQI_CATEGORIES, range=AQI_COLORS),
                            sort=AQI_CATEGORIES),
            order=alt.Order("aqi_rank:Q"),
            tooltip=["city_name", "aqi_category", alt.Tooltip("size:Q", title="hours")],
        )
        .transform_calculate(aqi_rank=f"indexof({AQI_CATEGORIES}, datum.aqi_category)")
    )
    st.altair_chart(stacked.properties(height=300), use_container_width=True)

st.subheader("Haze episodes (3+ consecutive hours with US AQI > 100)")
shown = episodes[episodes["city_name"].isin(selected)]
if shown.empty:
    st.info("No haze episodes in the selected window. 🎉")
else:
    st.dataframe(shown.drop(columns=["city_id"]), hide_index=True, use_container_width=True)

with st.expander("Data quality — latest run"):
    st.dataframe(dq, hide_index=True, use_container_width=True)
    st.caption(f"{int(dq['passed'].sum())} of {len(dq)} checks passed. Error-severity failures stop the pipeline "
               "before data reaches this dashboard.")
