import streamlit as st
from data import get_regional_metrics, get_sessionization, get_silver
import figures
import pandas as pd

st.set_page_config(layout="wide")

st.title("Greek Internet Analytics  :material/android_wifi_4_bar: ")

with st.spinner("Fetching data from BigQuery..."):
    df_sessionization = get_sessionization()
    df_region = get_regional_metrics()
    df_region["measurement_date"] = pd.to_datetime(df_region["measurement_date"])
    df_region = df_region.sort_values("measurement_date")

col1a, col1b = st.columns([0.15, 0.85])

with col1a:
    ############ POLARS #####################
    # selected_region = st.multiselect(
    #    "Select peripheries:",
    #    options=df_region["connection_periphery"].unique().sort().to_list(),
    #    default=["N/A", "ΑΤΤΙΚΗΣ", "ΚΕΝΤΡΙΚΗΣ ΜΑΚΕΔΟΝΙΑΣ"]
    # )
    # --- 3. Filter Data ---
    # subset_df = df_region.filter(pl.col("connection_periphery").is_in(selected_region)) if selected_region else df_region
    with st.container(border=True, height="stretch", vertical_alignment="center"):
        selected_region = st.multiselect(
            "Select peripheries:",
            options=df_region["connection_periphery"].unique(),
            default=["N/A", "ΑΤΤΙΚΗΣ", "ΚΕΝΤΡΙΚΗΣ ΜΑΚΕΔΟΝΙΑΣ"],
        )

        YEARS = df_region["measurement_date"].dt.year.unique()

        start_year, end_year = st.slider(
            "Select a range of values",
            YEARS.min(),
            YEARS.max(),
            (YEARS.max() - 4, YEARS.max()),
        )
        selected_years = list(range(start_year, end_year + 1))

        # --- 3. Filter Data ---
        subset_df = (
            df_region[
                (df_region["connection_periphery"].isin(selected_region))
                & (df_region["measurement_date"].dt.year.isin(selected_years))
            ]
            if selected_region
            else df_region
        )

    with st.container(border=True, height="stretch", vertical_alignment="center"):
        st.metric(
            "Average downstream speed",
            f"{(sum(subset_df['avg_downstream_mbps'] * subset_df['total_tests']) / subset_df['total_tests'].sum()):.2f} MB/s",
            # max_norm_value[1],
            # delta=f"{round(max_norm_value[0] * 100)}%",
            # width="content",
        )
        st.metric(
            "Average upstream speed",
            f"{(sum(subset_df['avg_upstream_mbps'] * subset_df['total_tests']) / subset_df['total_tests'].sum()):.2f} MB/s",
        )
        st.metric("Total speed tests", f"{subset_df['total_tests'].sum()}")

with col1b:
    with st.container(border=True, height="stretch", vertical_alignment="center"):
        options = ["Download", "Upload"]
        selection = st.pills(
            "Steam", options, selection_mode="single", default=options[0]
        )

        chart = figures.get_line(subset_df, case=selection)
        st.altair_chart(chart, width="stretch")

col2a, col2b = st.columns([0.5, 0.5], border=True)

with col2a:
    chart = figures.get_marks(subset_df)
    st.altair_chart(chart, width="stretch")

with col2b:
    chart = figures.get_tests(subset_df)
    st.altair_chart(chart, width="stretch")


"""
## User Behavioral Analytics 
A session is defined as............................................
"""

col3a, col3b = st.columns([0.5, 0.5], border=True)

with col3a:
    chart = figures.get_hist_chart(df_sessionization)
    st.altair_chart(chart, width="stretch")

with col3b:
    chart = figures.get_hist_chart2(df_sessionization)
    st.altair_chart(chart, width="stretch")