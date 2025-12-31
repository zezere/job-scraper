from typing import Optional
import streamlit as st
import pandas as pd
from db_connection import get_connection
from utils import validate_query_result


def get_processed_jobs() -> Optional[pd.DataFrame]:
    """
    Fetches all jobs from processedjobs table.

    Returns:
        DataFrame with all processed jobs, or None if error
    """
    try:
        with get_connection() as (conn, cursor):
            query = "SELECT * FROM public.processedjobs ORDER BY seen_first_at DESC"
            cursor.execute(query)
            rows = cursor.fetchall()
            column_names = [desc[0] for desc in cursor.description]

            df = pd.DataFrame(rows, columns=column_names)

            if not df.empty:
                validate_query_result(df, ["title"], get_processed_jobs.__name__)

            return df

    except Exception as e:
        st.error(f"Error fetching data from database: {e}")
        return None


def main() -> None:
    """
    Main Streamlit application entry point.

    Displays a dashboard for viewing and filtering tech jobs in Europe.
    Includes filters for is_remote, job_level, job_type, and job_function.
    """
    st.set_page_config(page_title="Tech Jobs in Europe", page_icon="🇪🇺", layout="wide")

    st.markdown(
        """
        <style>
        div[data-testid="column"]:nth-of-type(2) {
            display: flex;
            align-items: flex-end;
            justify-content: flex-end;
            padding-left: 0;
            padding-right: 0;
        }
        
        div[data-testid="column"]:nth-of-type(2) .element-container {
            padding-right: 0 !important;
        }
        
        div[data-testid="stDataFrame"] {
            padding-right: 0;
        }
        
        .element-container:has(div[data-testid="stDataFrame"]) {
            padding-right: 0 !important;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

    st.title("Tech Jobs in Europe 🇪🇺")

    col1, col2 = st.columns([4, 1])
    with col1:
        st.markdown(
            "Power over to you: view all tech jobs in Europe, filter, sort, export"
        )
    with col2:
        if st.button("Refresh Data", use_container_width=True):
            st.cache_data.clear()

    df = get_processed_jobs()

    if df is None:
        st.error("Failed to load data. Please check the database connection.")
        return

    if df.empty:
        st.info("No jobs in the database.")
        return

    with st.sidebar:
        st.header("Filters")

        if "is_remote" in df.columns:
            remote_filter = st.selectbox(
                "Remote Work",
                options=["All", "Remote", "Not Remote"],
                index=0,
            )
        else:
            remote_filter = "All"

        if "job_level" in df.columns:
            job_level_options = [
                "internship",
                "entry level",
                "associate",
                "mid-senior level",
                "director",
                "executive",
                "not applicable",
            ]
            available_job_levels = [
                level for level in job_level_options if level in df["job_level"].values
            ]
            if available_job_levels:
                job_level_filter = st.multiselect(
                    "Job Level",
                    options=available_job_levels,
                    default=available_job_levels,
                )
            else:
                job_level_filter = []
        else:
            job_level_filter = []

        if "job_type" in df.columns:
            job_type_options = ["fulltime", "parttime", "contract", "internship"]
            available_job_types = [
                jtype for jtype in job_type_options if jtype in df["job_type"].values
            ]
            if available_job_types:
                job_type_filter = st.multiselect(
                    "Job Type",
                    options=available_job_types,
                    default=available_job_types,
                )
            else:
                job_type_filter = []
        else:
            job_type_filter = []

        if "job_function" in df.columns:
            available_job_functions = sorted(
                df["job_function"].dropna().unique().tolist()
            )
            if available_job_functions:
                job_function_filter = st.multiselect(
                    "Job Function",
                    options=available_job_functions,
                    default=available_job_functions,
                )
            else:
                job_function_filter = []
        else:
            job_function_filter = []

    filtered_df = df.copy()

    if "is_remote" in df.columns and remote_filter != "All":
        if remote_filter == "Remote":
            filtered_df = filtered_df[filtered_df["is_remote"] == True]
        elif remote_filter == "Not Remote":
            filtered_df = filtered_df[filtered_df["is_remote"] == False]

    if "job_level" in df.columns and job_level_filter:
        filtered_df = filtered_df[filtered_df["job_level"].isin(job_level_filter)]

    if "job_type" in df.columns and job_type_filter:
        filtered_df = filtered_df[filtered_df["job_type"].isin(job_type_filter)]

    if "job_function" in df.columns and job_function_filter:
        filtered_df = filtered_df[filtered_df["job_function"].isin(job_function_filter)]

    # st.metric("Total Jobs", len(df))

    columns_to_show = [
        "title",
        "is_remote",
        "company",
        "location",
        "company_industry",
        "job_level",
        "job_type",
        "job_function",
        "description",
        "company_url",
        # "date_posted", - comes as null, TODO: find out why
        "job_url",
        "job_url_direct",
    ]

    available_columns = [col for col in columns_to_show if col in df.columns]
    display_df = filtered_df[available_columns]

    st.dataframe(display_df, use_container_width=True, hide_index=True)


if __name__ == "__main__":
    main()
