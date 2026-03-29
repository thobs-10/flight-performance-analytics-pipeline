"""Streamlit dashboard entrypoint for gold-layer flight insights."""

from __future__ import annotations

import os
import sys
from typing import Any

import clickhouse_connect
import pandas as pd
from dotenv import load_dotenv
from flight_performance_analytics_pipeline.logging.logging import Logger


def _running_from_streamlit() -> bool:
    """Return True when executed via `streamlit run`.

    This lets `python main.py` stay useful for local CLI checks.
    """
    try:
        from streamlit.runtime.scriptrunner import get_script_run_ctx

        if get_script_run_ctx() is not None:
            return True
    except Exception:  # nosec B110
        # Streamlit is not available or not running this script via streamlit.
        pass

    return bool(os.getenv("STREAMLIT_SERVER_PORT")) or any(
        "streamlit" in arg.lower() for arg in sys.argv
    )


def _get_clickhouse_client() -> Any:
    """Create a ClickHouse client from environment variables."""
    load_dotenv()
    return clickhouse_connect.get_client(
        host=os.getenv("CLICKHOUSE_HOST", "localhost"),
        port=int(os.getenv("CLICKHOUSE_PORT", "8123")),
        username=os.getenv("CLICKHOUSE_USER", "default"),
        password=os.getenv("CLICKHOUSE_PASSWORD", "clickhouse"),
        database=os.getenv("CLICKHOUSE_DATABASE", "gold"),
    )


def _query_dataframe(query: str) -> pd.DataFrame:
    """Execute a ClickHouse query and return a pandas DataFrame."""
    client = _get_clickhouse_client()
    try:
        return client.query_df(query)
    finally:
        client.close()


def _run_streamlit_dashboard() -> None:
    """Render the one-page dashboard with two gold-layer insight tiles."""
    import streamlit as st

    st.set_page_config(
        page_title="Flight Performance Insights",
        page_icon=":airplane_departure:",
        layout="wide",
    )

    st.title("Flight Performance Insights")
    st.caption("Live metrics from ClickHouse gold materialized views")

    @st.cache_data(ttl=60 * 60 * 24)
    def get_carrier_performance() -> pd.DataFrame:
        return _query_dataframe(
            """
            SELECT
                carrier,
                sumMerge(total_flights_state) AS total_flights,
                sumMerge(total_delayed_state) AS total_delayed,
                round(100.0 * total_delayed / nullIf(total_flights, 0), 2) AS delay_rate_pct
            FROM gold.mv_carrier_delay_performance
            GROUP BY carrier
            HAVING total_flights > 0
            ORDER BY delay_rate_pct DESC, total_delayed DESC
            LIMIT 10
            """
        )

    @st.cache_data(ttl=60 * 60 * 24)
    def get_monthly_trends() -> pd.DataFrame:
        return _query_dataframe(
            """
            SELECT
                toDate(concat(toString(year), '-', lpad(toString(month), 2, '0'), '-01')) AS month_start,
                sumMerge(total_flights_state) AS total_flights,
                sumMerge(total_delayed_state) AS total_delayed,
                round(100.0 * total_delayed / nullIf(total_flights, 0), 2) AS delay_rate_pct
            FROM gold.mv_monthly_delay_trends
            GROUP BY month_start
            ORDER BY month_start
            """
        )

    left_col, right_col = st.columns(2)

    with left_col:
        st.subheader("Top 10 carriers by delay rate")
        carrier_df = get_carrier_performance()
        if carrier_df.empty:
            st.info("No carrier performance data available yet.")
        else:
            st.metric(
                label="Highest delay rate",
                value=f"{carrier_df.iloc[0]['delay_rate_pct']:.2f}%",
                delta=str(carrier_df.iloc[0]["carrier"]),
            )
            st.bar_chart(
                carrier_df.set_index("carrier")[["delay_rate_pct"]],
                y_label="Delay rate (%)",
                x_label="Carrier",
                color="#C83E4D",
            )
            st.dataframe(carrier_df, use_container_width=True, hide_index=True)

    with right_col:
        st.subheader("Monthly delay trend")
        trends_df = get_monthly_trends()
        if trends_df.empty:
            st.info("No monthly trend data available yet.")
        else:
            latest = trends_df.iloc[-1]
            st.metric(
                label="Latest monthly delay rate",
                value=f"{latest['delay_rate_pct']:.2f}%",
                delta=latest["month_start"].strftime("%Y-%m"),
            )
            chart_df = trends_df.set_index("month_start")[["delay_rate_pct"]]
            st.line_chart(
                chart_df,
                y_label="Delay rate (%)",
                x_label="Month",
                color="#1E6A8D",
            )
            st.dataframe(trends_df, use_container_width=True, hide_index=True)


def main() -> None:
    """Run dashboard in Streamlit, otherwise print local CLI guidance."""
    if _running_from_streamlit():
        _run_streamlit_dashboard()
        return

    log = Logger()
    log.info("Dashboard entrypoint is ready.")
    log.info("Run with: streamlit run main.py")


if __name__ == "__main__":
    main()
