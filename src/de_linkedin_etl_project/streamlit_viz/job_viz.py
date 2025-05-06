import pandas as pd
import streamlit as st
import snowflake.connector
import plotly_express as px
import os

st.set_page_config(layout="wide")

#Snowflake connection
def connect_to_snowflake():
    conn = snowflake.connector.connect(
        user="NIKKILW2025",
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        account="gbszkwp-by30611",
        warehouse="SNOWFLAKE_LEARNING_WH",
        database="linkedin_db",
        schema="linkedin_raw"
    )
    return conn

#Title
##Get date ranges for data reports
def job_dates(conn):
    query = """
        SELECT MIN(DATE) AS min_date, MAX(DATE) AS max_date
        FROM mart_total_jobs_daily
    """
    df_dates = pd.read_sql(query, conn)
    return df_dates

## title and date range
def title(df_dates):
    min_date = df_dates['MIN_DATE'][0]
    max_date = df_dates['MAX_DATE'][0]
    st.markdown(
        f"""
        <div style="text-align:center;margin-bottom:2rem;">
        <div style='font-size:2.5rem;font-weight:600;margin-bottom:0;margin-top:0;'>Australian Data Job Trends on LinkedIn</div>
        <div style='font-size:1.3rem;font-weight:400;margin-top:0.3em;margin-bottom:0;'> {min_date} - {max_date}</div>
        <br>
        <div style='font-size:1.4rem;font-weight:100;margin-top:0.3em;margin-bottom:0;'> Total Jobs By Type </div>
        </div>
        """,
        unsafe_allow_html=True
    )


## Section 1 - Job Data Summary

##Get total jobs by title
def query_job_total(conn):
    query = """
        SELECT JOB_CATEGORY as Title,
        COUNT(DISTINCT ID) as Total_Jobs
        FROM LINKEDIN_JOB_API_CLEANED_DATA
        WHERE
        lower(title) LIKE '%data engineer%'
        or lower(title) LIKE '%data analyst%'
        or lower(title) LIKE '%data scientist%'
        GROUP BY JOB_CATEGORY
        ORDER BY Title ASC
    """
    df_job_total = pd.read_sql(query, conn)
    return df_job_total


## Dashboard viz - total job summary
def viz_job_data_summary(df_job_total):
    col1, col2, col3 = st.columns(3)
    with col1:
        st.markdown(
            f"<div style='text-align:center;'><span style='font-size:50px;font-weight:bold;color:#0099ff;'>{int(df_job_total.iloc[0,1])}</span><br><b>Data Analyst</b></div>",
            unsafe_allow_html=True)
    with col2:
        st.markdown(
            f"<div style='text-align:center;'><span style='font-size:50px;font-weight:bold;color:#0099ff;'>{int(df_job_total.iloc[1,1])}</span><br><b>Data Engineer</b></div>",
            unsafe_allow_html=True)
    with col3:
        st.markdown(
            f"<div style='text-align:center;'><span style='font-size:50px;font-weight:bold;color:#0099ff;'>{int(df_job_total.iloc[2,1])}</span><br><b>Data Scientist</b></div>",
            unsafe_allow_html=True)



## Section 2 - Daily Job Trend
def viz_daily_job_data(conn):
    # 查询数据
    def query_daily_job_data(conn):
        query = """
            SELECT
            DATE,
            JOB_CATEGORY as Title,
            SUM(TOTAL_JOBS) as Total_Jobs
            FROM MART_TOTAL_JOBS_DAILY
            WHERE
            lower(title) LIKE '%data engineer%'
            or lower(title) LIKE '%data analyst%'
            or lower(title) LIKE '%data scientist%'
            GROUP BY Title, DATE
            ORDER BY Title, DATE ASC
        """
        df_daily_jobs = pd.read_sql(query, conn)
        return df_daily_jobs

    df_daily_jobs = query_daily_job_data(conn)

    st.markdown(
        """
        <div style='text-align:center; font-size:1.4rem; font-weight:400; margin-top:1.4em; margin-bottom:1em;'>
        Daily Job Trend by Role
        </div>
        """,
        unsafe_allow_html=True
    )

    job_titles = ["Data Analyst", "Data Engineer", "Data Scientist"]
    color_map = {
        "Data Analyst": "#1f77b4",
        "Data Engineer": "#d62728",
        "Data Scientist": "#aec7e8",
    }

    cols = st.columns(3)
    for i, job in enumerate(job_titles):
        df_job = df_daily_jobs[df_daily_jobs["TITLE"] == job]
        fig = px.line(
            df_job,
            x="DATE",
            y="TOTAL_JOBS",
            title=job,
            color_discrete_sequence=[color_map[job]]
        )
        fig.update_layout(
            showlegend=False,
            title_x=0.5,
            margin=dict(l=10, r=10, t=40, b=10),
            height = 300,
            width = 420
        )
        cols[i].plotly_chart(fig, use_container_width=False)




def main():
    conn = connect_to_snowflake()
    df_dates = job_dates(conn)
    title(df_dates)
    df_job_total = query_job_total(conn)
    viz_job_data_summary(df_job_total)
    viz_daily_job_data(conn)


if __name__ == "__main__":
    main()
