import streamlit as st
import pandas as pd
from snowflake.snowpark.context import get_active_session

session = get_active_session()
st.set_page_config(page_title="CityRide Dashboard", layout="wide")

def q(sql):
    return session.sql(sql).to_pandas()

# no filters

# ── title ─────────────────────────────────────────────────────
st.title("CityRide — Data Platform Dashboard")

tab1, tab2, tab3, tab4, tab5, tab6, tab7 = st.tabs([
    "Overview",
    "KPI 1 — Anomaly",
    "KPI 2 — Availability",
    "KPI 3 — Engagement",
    "KPI 4 — Fleet Health",
    "KPI 5 — Revenue",
    "Date Analytics"
])

# ============================================================
# TAB 1 — OVERVIEW
# ============================================================
with tab1:
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total Rentals",    q(f"SELECT COUNT(*) FROM CITYRIDE_DB.CURATED.FACT_RENTAL f WHERE 1=1").iloc[0,0])
    c2.metric("Active Stations",  q("SELECT COUNT(*) FROM CITYRIDE_DB.CURATED.DIM_STATION WHERE IS_CURRENT=TRUE").iloc[0,0])
    c3.metric("Active Bikes",     q("SELECT COUNT(*) FROM CITYRIDE_DB.CURATED.DIM_BIKE WHERE IS_CURRENT=TRUE").iloc[0,0])
    c4.metric("Registered Users", q("SELECT COUNT(*) FROM CITYRIDE_DB.CURATED.DIM_USER WHERE IS_CURRENT=TRUE").iloc[0,0])

    st.divider()
    cl, cr = st.columns(2)

    with cl:
        st.subheader("Rentals over time")
        rt = q(f"""
            SELECT f.start_time::DATE AS d, COUNT(*) AS rentals
            FROM CITYRIDE_DB.CURATED.FACT_RENTAL f
            WHERE 1=1
            GROUP BY 1 ORDER BY 1
        """)
        if not rt.empty:
            st.line_chart(rt.set_index("D")["RENTALS"])

    with cr:
        st.subheader("Revenue over time")
        rv = q(f"""
            SELECT f.start_time::DATE AS d, ROUND(SUM(f.price),2) AS revenue
            FROM CITYRIDE_DB.CURATED.FACT_RENTAL f
            WHERE 1=1
            GROUP BY 1 ORDER BY 1
        """)
        if not rv.empty:
            st.line_chart(rv.set_index("D")["REVENUE"])

    st.divider()
    st.subheader("SCD2 version history")
    sc1, sc2 = st.columns(2)

    with sc1:
        st.caption("Station versions")
        st.dataframe(q("""
            SELECT station_id, capacity, city_zone,
                   effective_from::DATE AS from_date,
                   COALESCE(effective_to::DATE::VARCHAR,'current') AS to_date,
                   is_current
            FROM CITYRIDE_DB.CURATED.DIM_STATION
            ORDER BY station_id, effective_from
        """), width='stretch')

    with sc2:
        st.caption("User versions")
        st.dataframe(q("""
            SELECT user_id, email, region,
                   effective_from::DATE AS from_date,
                   COALESCE(effective_to::DATE::VARCHAR,'current') AS to_date,
                   is_current
            FROM CITYRIDE_DB.CURATED.DIM_USER
            ORDER BY user_id, effective_from
        """), width='stretch')

    st.divider()
    st.subheader("Audit log")
    st.dataframe(q("""
        SELECT STARTED_AT::DATE AS date, LAYER, DOMAIN, OPERATION,
               ROWS_PROCESSED, ROWS_INSERTED, STATUS, ERROR_MESSAGE
        FROM CITYRIDE_DB.ANALYTICS.AUDIT_LOG
        ORDER BY STARTED_AT DESC LIMIT 15
    """), width='stretch')


# ============================================================
# TAB 2 — KPI 1: ANOMALY
# ============================================================
with tab2:
    st.subheader("KPI 1 — Anomalous Rental Probability Score")
    st.caption("(Flagged rentals / Total rentals) × 100")

    df1 = q("SELECT SNAPSHOT_DATE, TOTAL_RENTALS, FLAGGED_RENTALS, ANOMALY_PROBABILITY FROM CITYRIDE_DB.ANALYTICS.KPI_ANOMALOUS_RENTAL_SCORE ORDER BY SNAPSHOT_DATE")

    if df1.empty:
        st.info("No data yet.")
    else:
        c1, c2, c3 = st.columns(3)
        c1.metric("Total Rentals",   f"{df1.iloc[-1]['TOTAL_RENTALS']:,}")
        c2.metric("Flagged Rentals", f"{df1.iloc[-1]['FLAGGED_RENTALS']:,}")
        c3.metric("Anomaly Score",   f"{df1.iloc[-1]['ANOMALY_PROBABILITY']}%",
                  delta=f"{round(df1.iloc[-1]['ANOMALY_PROBABILITY']-df1.iloc[-2]['ANOMALY_PROBABILITY'],2)}%" if len(df1)>1 else None)
        st.line_chart(df1.set_index("SNAPSHOT_DATE")["ANOMALY_PROBABILITY"], color="#E24B4A")

    st.divider()
    st.subheader("Flagged rentals")
    st.dataframe(q(f"""
        SELECT f.rental_id, f.start_time::DATE AS date, f.channel,
               f.plan_type, f.price, f.duration_sec, f.distance_km, f.anomaly_score
        FROM CITYRIDE_DB.CURATED.FACT_RENTAL f
        WHERE f.is_flagged = TRUE
        ORDER BY f.start_time DESC LIMIT 50
    """), width='stretch')


# ============================================================
# TAB 3 — KPI 2: AVAILABILITY
# ============================================================
with tab3:
    st.subheader("KPI 2 — Station Availability Score")
    st.caption("% of time station has ≥1 bike AND ≥1 free dock")

    df2 = q(f"""
        SELECT k.station_id, k.city_zone, k.pct_time_available,
               k.avg_bikes_available, k.avg_docks_free
        FROM CITYRIDE_DB.ANALYTICS.KPI_STATION_AVAILABILITY k
        WHERE k.snapshot_date = (SELECT MAX(snapshot_date) FROM CITYRIDE_DB.ANALYTICS.KPI_STATION_AVAILABILITY)
                ORDER BY k.pct_time_available ASC
    """)

    if df2.empty:
        st.info("No data yet.")
    else:
        c1, c2 = st.columns(2)
        c1.metric("Avg Availability",    f"{round(df2['PCT_TIME_AVAILABLE'].mean(),1)}%")
        c2.metric("Stations monitored",  len(df2))

        cl, cr = st.columns(2)
        with cl:
            st.caption("Availability % per station")
            st.bar_chart(df2.set_index("STATION_ID")["PCT_TIME_AVAILABLE"])
        with cr:
            st.caption("Avg bikes available")
            st.bar_chart(df2.set_index("STATION_ID")["AVG_BIKES_AVAILABLE"])

        low = df2[df2["PCT_TIME_AVAILABLE"] < 50]
        if not low.empty:
            st.warning(f"{len(low)} stations below 50% — rebalancing needed")
            st.dataframe(low, width='stretch')
        else:
            st.success("All stations above 50% availability")

        st.dataframe(df2, width='stretch')


# ============================================================
# TAB 4 — KPI 3: ENGAGEMENT
# ============================================================
with tab4:
    st.subheader("KPI 3 — Active Rider Engagement Ratio")
    st.caption("% of registered riders with ≥1 rental in last 30 days")

    df3 = q("SELECT SNAPSHOT_DATE, TOTAL_REGISTERED, ACTIVE_LAST_30D, ENGAGEMENT_RATIO FROM CITYRIDE_DB.ANALYTICS.KPI_RIDER_ENGAGEMENT ORDER BY SNAPSHOT_DATE")

    if df3.empty:
        st.info("No data yet.")
    else:
        c1, c2, c3 = st.columns(3)
        c1.metric("Registered", f"{df3.iloc[-1]['TOTAL_REGISTERED']:,}")
        c2.metric("Active 30d", f"{df3.iloc[-1]['ACTIVE_LAST_30D']:,}")
        c3.metric("Ratio",      f"{df3.iloc[-1]['ENGAGEMENT_RATIO']}%")
        st.area_chart(df3.set_index("SNAPSHOT_DATE")["ENGAGEMENT_RATIO"], color="#1D9E75")

    st.divider()
    st.subheader("Rider activity breakdown")
    st.dataframe(q(f"""
        SELECT du.user_id, du.region, COUNT(f.rental_sk) AS total_rentals,
               MAX(f.start_time)::DATE AS last_rental,
               ROUND(SUM(f.price),2)  AS total_spent
        FROM CITYRIDE_DB.CURATED.DIM_USER du
        LEFT JOIN CITYRIDE_DB.CURATED.FACT_RENTAL f ON f.user_sk = du.user_sk
        WHERE du.is_current = TRUE
        GROUP BY du.user_id, du.region
        ORDER BY total_rentals DESC
    """), width='stretch')


# ============================================================
# TAB 5 — KPI 4: FLEET HEALTH
# ============================================================
with tab5:
    st.subheader("KPI 4 — Fleet Maintenance Health Index")
    st.caption("% of bikes within health thresholds")

    df4 = q("SELECT SNAPSHOT_DATE, TOTAL_BIKES, HEALTHY_BIKES, HEALTH_INDEX, EBIKES_LOW_BATTERY, BIKES_OVERDUE_SERVICE FROM CITYRIDE_DB.ANALYTICS.KPI_FLEET_HEALTH ORDER BY SNAPSHOT_DATE")

    if df4.empty:
        st.info("No data yet.")
    else:
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Total Bikes",    f"{df4.iloc[-1]['TOTAL_BIKES']:,}")
        c2.metric("Healthy",        f"{df4.iloc[-1]['HEALTHY_BIKES']:,}")
        c3.metric("Health Index",   f"{df4.iloc[-1]['HEALTH_INDEX']}%")
        c4.metric("Low Battery",    f"{df4.iloc[-1]['EBIKES_LOW_BATTERY']:,}")
        if df4.iloc[-1]['BIKES_OVERDUE_SERVICE'] > 0:
            st.warning(f"{int(df4.iloc[-1]['BIKES_OVERDUE_SERVICE'])} bikes overdue for service")
        st.line_chart(df4.set_index("SNAPSHOT_DATE")["HEALTH_INDEX"], color="#378ADD")

    st.divider()
    cl, cr = st.columns(2)
    with cl:
        st.subheader("Status distribution")
        st.bar_chart(q("""
            SELECT STATUS, COUNT(*) AS n FROM CITYRIDE_DB.CURATED.DIM_BIKE
            WHERE IS_CURRENT=TRUE GROUP BY STATUS
        """).set_index("STATUS")["N"])

    with cr:
        st.subheader("eBike battery levels")
        st.dataframe(q("""
            SELECT BIKE_ID, BATTERY_LEVEL, STATUS, ODOMETER_KM
            FROM CITYRIDE_DB.CURATED.DIM_BIKE
            WHERE IS_CURRENT=TRUE AND BIKE_TYPE='ebike'
            ORDER BY BATTERY_LEVEL ASC
        """), width='stretch')

    st.subheader("SCD2 — bikes with version history (inc changes)")
    bh = q("""
        SELECT bike_id, bike_type, status, battery_level, odometer_km,
               effective_from::DATE AS from_date,
               COALESCE(effective_to::DATE::VARCHAR,'current') AS to_date,
               is_current
        FROM CITYRIDE_DB.CURATED.DIM_BIKE
        WHERE bike_id IN (SELECT bike_id FROM CITYRIDE_DB.CURATED.DIM_BIKE GROUP BY bike_id HAVING COUNT(*)>1)
        ORDER BY bike_id, effective_from
    """)
    if bh.empty:
        st.info("No SCD2 changes yet.")
    else:
        st.dataframe(bh, width='stretch')


# ============================================================
# TAB 6 — KPI 5: REVENUE
# ============================================================
with tab6:
    st.subheader("KPI 5 — Average Rental Revenue by Channel")

    df5 = q(f"""
        SELECT CHANNEL, PLAN_TYPE, TOTAL_RENTALS, TOTAL_REVENUE, AVG_RENTAL_REV
        FROM CITYRIDE_DB.ANALYTICS.KPI_ARR_BY_CHANNEL
        WHERE SNAPSHOT_DATE=(SELECT MAX(SNAPSHOT_DATE) FROM CITYRIDE_DB.ANALYTICS.KPI_ARR_BY_CHANNEL)
                ORDER BY TOTAL_REVENUE DESC
    """)

    if df5.empty:
        st.info("No data yet.")
    else:
        c1, c2, c3 = st.columns(3)
        c1.metric("Total Revenue",  f"₹{df5['TOTAL_REVENUE'].sum():,.2f}")
        c2.metric("Total Rentals",  f"{df5['TOTAL_RENTALS'].sum():,}")
        c3.metric("Avg per Rental", f"₹{round(df5['TOTAL_REVENUE'].sum()/max(df5['TOTAL_RENTALS'].sum(),1),2):,.2f}")

        cl, cr = st.columns(2)
        with cl:
            st.caption("Revenue by channel")
            st.bar_chart(df5.groupby("CHANNEL")["TOTAL_REVENUE"].sum().reset_index().set_index("CHANNEL"), color="#534AB7")
        with cr:
            st.caption("Revenue by plan type")
            st.bar_chart(df5.groupby("PLAN_TYPE")["TOTAL_REVENUE"].sum().reset_index().set_index("PLAN_TYPE"), color="#BA7517")

        st.subheader("Revenue trend by channel")
        rh = q(f"""
            SELECT f.start_time::DATE AS d, f.channel, ROUND(SUM(f.price),2) AS revenue
            FROM CITYRIDE_DB.CURATED.FACT_RENTAL f
            WHERE 1=1
            GROUP BY 1,2 ORDER BY 1
        """)
        if not rh.empty:
            st.line_chart(rh.pivot(index="D", columns="CHANNEL", values="REVENUE").fillna(0))

        st.dataframe(df5, width='stretch')


# ============================================================
# TAB 7 — DATE ANALYTICS (DIM_DATE)
# ============================================================
with tab7:
    st.subheader("Date Dimension Analytics")
    st.caption("Powered by DIM_DATE — 2020 to 2030 calendar")

    c1, c2, c3 = st.columns(3)
    c1.metric("Calendar rows", q("SELECT COUNT(*) FROM CITYRIDE_DB.CURATED.DIM_DATE").iloc[0,0])
    c2.metric("From", str(q("SELECT MIN(full_date) FROM CITYRIDE_DB.CURATED.DIM_DATE").iloc[0,0]))
    c3.metric("To",   str(q("SELECT MAX(full_date) FROM CITYRIDE_DB.CURATED.DIM_DATE").iloc[0,0]))

    st.divider()

    cl, cr = st.columns(2)

    with cl:
        st.subheader("Rentals by day of week")
        dow = q(f"""
            SELECT d.day_name, d.day_of_week, COUNT(f.rental_sk) AS rentals
            FROM CITYRIDE_DB.CURATED.FACT_RENTAL f
            JOIN CITYRIDE_DB.CURATED.DIM_DATE d ON d.date_sk = f.date_sk
            WHERE 1=1
            GROUP BY d.day_name, d.day_of_week
            ORDER BY d.day_of_week
        """)
        if not dow.empty:
            st.bar_chart(dow.set_index("DAY_NAME")["RENTALS"], color="#7F77DD")

    with cr:
        st.subheader("Rentals by month")
        mom = q(f"""
            SELECT d.month_name, d.month, COUNT(f.rental_sk) AS rentals
            FROM CITYRIDE_DB.CURATED.FACT_RENTAL f
            JOIN CITYRIDE_DB.CURATED.DIM_DATE d ON d.date_sk = f.date_sk
            WHERE 1=1
            GROUP BY d.month_name, d.month
            ORDER BY d.month
        """)
        if not mom.empty:
            st.bar_chart(mom.set_index("MONTH_NAME")["RENTALS"], color="#1D9E75")

    st.divider()
    cl2, cr2 = st.columns(2)

    with cl2:
        st.subheader("Weekday vs weekend rentals")
        wknd = q(f"""
            SELECT IFF(d.is_weekend, 'Weekend', 'Weekday') AS day_type,
                   COUNT(f.rental_sk) AS rentals,
                   ROUND(SUM(f.price),2) AS revenue
            FROM CITYRIDE_DB.CURATED.FACT_RENTAL f
            JOIN CITYRIDE_DB.CURATED.DIM_DATE d ON d.date_sk = f.date_sk
            WHERE 1=1
            GROUP BY d.is_weekend
        """)
        if not wknd.empty:
            st.bar_chart(wknd.set_index("DAY_TYPE")[["RENTALS","REVENUE"]])

    with cr2:
        st.subheader("Rentals by quarter")
        qtr = q(f"""
            SELECT d.year, d.quarter,
                   CONCAT(d.year::VARCHAR,' Q',d.quarter::VARCHAR) AS period,
                   COUNT(f.rental_sk) AS rentals,
                   ROUND(SUM(f.price),2) AS revenue
            FROM CITYRIDE_DB.CURATED.FACT_RENTAL f
            JOIN CITYRIDE_DB.CURATED.DIM_DATE d ON d.date_sk = f.date_sk
            WHERE 1=1
            GROUP BY d.year, d.quarter, period
            ORDER BY d.year, d.quarter
        """)
        if not qtr.empty:
            st.bar_chart(qtr.set_index("PERIOD")["RENTALS"], color="#EF9F27")
