import pandas as pd
from datetime import datetime, timedelta
import streamlit as st
import snowflake.connector
import plotly.express as px
import plotly.graph_objects as go
import os

st.set_page_config(layout="wide")

# Snowflake connection
def connect_to_snowflake():
    return snowflake.connector.connect(
        user="NIKKILW2025",
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        account="gbszkwp-by30611",
        warehouse="SNOWFLAKE_LEARNING_WH",
        database="linkedin_db",
        schema="linkedin_raw"
    )

@st.cache_data
def job_dates(_conn):
    query = """
        SELECT MIN(DATE) AS MIN_DATE, MAX(DATE) AS MAX_DATE
        FROM mart_total_jobs_daily
    """
    df_dates = pd.read_sql(query, _conn)
    return df_dates

def title(df_dates):
    min_date = df_dates.iloc[0]['MIN_DATE']
    max_date = df_dates.iloc[0]['MAX_DATE']
    st.markdown(
        f"""
        <div style="text-align:center; margin-bottom:2rem;">
            <div style='font-size:2.5rem; font-weight:600; margin-bottom:0; margin-top:0;'>
                Australian Data Job Trends on LinkedIn
            </div>
            <div style='font-size:1.3rem; font-weight:400; margin-top:0.3em; margin-bottom:0;'>
                {min_date} - {max_date}
            </div>
        </div>
        """,
        unsafe_allow_html=True)
    st.markdown(
        """
        <div style='text-align:center; font-size:1.65rem; font-weight:600; margin-bottom:0.7em; margin-top:0.7em;'>
            Total Jobs
            <hr style="border:1px solid #bbb; width:70%; margin:auto;">
        </div>
        """,
        unsafe_allow_html=True
    )

@st.cache_data
def query_job_total(_conn):
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
    df_job_total = pd.read_sql(query, _conn)
    return df_job_total

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

@st.cache_data
def query_wow_trend(_conn):
    query_wow = """
            SELECT
            WEEK_START,
            JOB_CATEGORY,
            THIS_WEEK_JOBS as THIS_WEEK,
            LAST_WEEK_JOBS as LAST_WEEK,
            WOW_DIFF_PCT
            FROM MART_TOTAL_JOBS_WOW_TREND
            ORDER BY WEEK_START DESC
            LIMIT 3
        """
    df_wow_trend = pd.read_sql(query_wow, _conn)
    return df_wow_trend

def viz_wow_trend(df_wow_trend):
    col1, col2, col3 = st.columns(3)
    for idx, role in enumerate(df_wow_trend['JOB_CATEGORY']):
        wow_pct = f"{df_wow_trend.loc[idx, 'WOW_DIFF_PCT'] * 100:.0f}%"
        value = int(df_wow_trend.loc[idx, 'THIS_WEEK'])
        with [col1, col2, col3][idx]:
            st.markdown(
                f"""
                <div style='text-align:center; margin-bottom:0.5em'>
                  <div style="font-size:2rem;color:#222; font-weight:600;">{value}</div>
                  <div style="font-size:1rem;font-weight:300;">This Week - WoW Diff (%)</div>
                  <div style="font-size:1.2rem; color:#d62728;">
                    &#8595; {wow_pct}
                  </div>
                </div>
                """,
                unsafe_allow_html=True
            )

@st.cache_data
def query_daily_job_data(_conn):
    query = """
        SELECT
        DATE,
        JOB_CATEGORY as TITLE,
        SUM(TOTAL_JOBS) as TOTAL_JOBS
        FROM MART_TOTAL_JOBS_DAILY
        WHERE
        lower(title) LIKE '%data engineer%'
        or lower(title) LIKE '%data analyst%'
        or lower(title) LIKE '%data scientist%'
        GROUP BY TITLE, DATE
        ORDER BY TITLE, DATE ASC
    """
    df_daily_jobs = pd.read_sql(query, _conn)
    return df_daily_jobs

def viz_daily_job_data(df_daily_jobs):
    st.markdown("""
    <div style='text-align:center; font-size:1.65rem; font-weight:600; margin-bottom:0.7em; margin-top:0.7em;'>
        Daily Job Trend
        <hr style="border:1px solid #bbb; width:70%; margin:auto;">
    </div>
    """, unsafe_allow_html=True)
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
            height=300,
            width=420
        )
        cols[i].plotly_chart(fig, use_container_width=False)

@st.cache_data
def query_salary_data(_conn):
    query = """
        SELECT
        JOB_TITLE,
        AVG(SALARY_RANGE_LOW) as SALARY_LOW,
        AVG(SALARY_RANGE_HIGH) as SALARY_HIGH
        FROM MART_JOB_SALARY
        GROUP BY JOB_TITLE
        ORDER BY JOB_TITLE ASC
    """
    df_salary = pd.read_sql(query, _conn)
    return df_salary

def viz_job_salary(df_salary):
    st.markdown("""
    <div style='text-align:center; font-size:1.65rem; font-weight:600; margin-bottom:0.7em; margin-top:0.7em;'>
        Salary by Job Role
        <hr style="border:1px solid #bbb; width:70%; margin:auto;">
    </div>
    """, unsafe_allow_html=True)
    col1, col2, col3 = st.columns(3)
    for idx, row in df_salary.iterrows():
        role = row['JOB_TITLE']
        min_salary = row['SALARY_LOW']
        max_salary = row['SALARY_HIGH']
        lower_bound = min_salary - (max_salary - min_salary)/2
        upper_bound = max_salary + (max_salary - min_salary)/2
        bar1 = min_salary - lower_bound
        bar2 = max_salary - min_salary
        bar3 = upper_bound - max_salary
        fig = go.Figure()
        fig.add_trace(go.Bar(
            x=[bar1],
            y=[role],
            orientation='h',
            marker_color='#f5f6fa',
            base=[lower_bound],
            hoverinfo='skip',
            showlegend=False,
        ))
        fig.add_trace(go.Bar(
            x=[bar2],
            y=[role],
            orientation='h',
            marker_color='#e6007a',
            base=[min_salary],
            hoverinfo='skip',
            showlegend=False,
            text=[f"${int(min_salary/1000)}K-${int(max_salary/1000)}K"],
            textposition='inside',
        ))
        fig.add_trace(go.Bar(
            x=[bar3],
            y=[role],
            orientation='h',
            marker_color='#f5f6fa',
            base=[max_salary],
            hoverinfo='skip',
            showlegend=False,
        ))
        fig.update_layout(
            barmode='relative',
            height=120,
            margin=dict(l=4, r=4, t=18, b=4),
            xaxis=dict(
                showgrid=False, showticklabels=True, zeroline=False,
                range=[lower_bound, upper_bound],
                title="Salary (AUD)"
            ),
            yaxis=dict(showticklabels=False),
            plot_bgcolor='white',
            paper_bgcolor='white'
        )
        with [col1, col2, col3][idx]:
            st.markdown(
                f"<div style='text-align:center; font-weight:400; margin-bottom:0.5rem'>{role}</div>", unsafe_allow_html=True
            )
            st.markdown(
                f"<div style='text-align:center;'>salary by state</div>", unsafe_allow_html=True
            )
            st.plotly_chart(fig, use_container_width=True)

@st.cache_data
def query_top_skills_data(_conn):
    query = """
        SELECT
        *
        FROM MART_TOP_JOB_SKILLS
        ORDER BY "Job_Title"
    """
    df_top_skills = pd.read_sql(query, _conn)
    return df_top_skills

def viz_top_job_skills(df_top_skills):
    st.markdown("""
    <div style='text-align:center; font-size:1.65rem; font-weight:600; margin-bottom:0.7em; margin-top:0.7em;'>
        Top Skills Required by Job Role
        <hr style="border:1px solid #bbb; width:70%; margin:auto;">
    </div>
    """, unsafe_allow_html=True)
    st.dataframe(df_top_skills, use_container_width=True)

@st.cache_data
def query_job_by_region_org(_conn):
    st.markdown("""
    <div style='text-align:center; font-size:1.65rem; font-weight:600; margin-bottom:0.7em; margin-top:0.7em;'>
        Job Stats by State, City & Organization
        <hr style="border:1px solid #bbb; width:70%; margin:auto;">
    </div>
    """, unsafe_allow_html=True)

    state_query = """
        SELECT * FROM MART_TOTAL_JOBS_BY_STATE
        ORDER BY
            CASE WHEN STATE = 'UNKNOWN' THEN 1 ELSE 0 END,
            STATE
    """
    df_state_jobs = pd.read_sql(state_query, _conn)

    city_query = """
        SELECT * FROM MART_TOTAL_JOBS_BY_CITY
        ORDER BY CITY, JOB_CATEGORY
    """
    df_city_jobs = pd.read_sql(city_query, _conn)

    org_query = """
        SELECT JOB_CATEGORY, ORGANIZATION, TOTAL_JOBS FROM MART_TOTAL_JOBS_BY_ORG
        ORDER BY ORGANIZATION, JOB_CATEGORY
    """
    df_org_jobs = pd.read_sql(org_query, _conn)
    return df_state_jobs, df_city_jobs, df_org_jobs

def viz_job_by_region_org(dfs):
    df_state_jobs, df_city_jobs, df_org_jobs = dfs

    #Job_category for 3 all dfs (dediplicated, sorted)
    all_job_categories = sorted(
        set(df_state_jobs['JOB_CATEGORY'].dropna().unique())
        | set(df_city_jobs['JOB_CATEGORY'].dropna().unique())
        | set(df_org_jobs['JOB_CATEGORY'].dropna().unique())
    )

    #Top Job Role Selector
    select_job_category=st.selectbox("Filter by Role", ['All'] + all_job_categories)

    #Filter function
    def filter_by_role(df):
        if select_job_category != 'All':
            return df[df['JOB_CATEGORY']==select_job_category]
        return df

    filtered_state =  filter_by_role(df_state_jobs)
    filtered_state = filtered_state.sort_values(by='TOTAL_JOBS', ascending=False)
    filtered_city =  filter_by_role(df_city_jobs)
    filtered_city = filtered_city.sort_values(by='TOTAL_JOBS', ascending=False)
    filtered_org =  filter_by_role(df_org_jobs)
    filtered_org = filtered_org.sort_values(by='TOTAL_JOBS', ascending=False)

    #Jobs by Role Table Viz
    col1, col2, col3 = st.columns(3)
    with col1:
        st.dataframe(filtered_state)
    with col2:
        st.dataframe(filtered_city)
    with col3:
        st.dataframe(filtered_org)



@st.cache_data
def query_job_details(_conn):
    st.markdown("""
    <div style='text-align:center; font-size:1.65rem; font-weight:600; margin-bottom:0.7em; margin-top:0.7em;'>
        Job Information
        <hr style="border:1px solid #bbb; width:70%; margin:auto;">
    </div>
    """, unsafe_allow_html=True)


    job_info_query = """
        SELECT DISTINCT JOB_CATEGORY AS Job_Role,
        TITLE, JOB_DATE AS Post_Date,
        EMPLOYMENT_TYPE, SENIORITY, CITY, STATE, ORGANIZATION,
        URL AS LinkedIn_Post_Link,
        LINKEDIN_ORG_URL,
        LINKEDIN_ORG_INDUSTRY,
        REMOTE_DERIVED AS IS_REMOTE,
        LINKEDIN_ORG_RECRUITMENT_AGENCY_DERIVED AS Job_By_Agency,
        DIRECTAPPLY
        FROM INT_LINKEDIN_DATA
        ORDER BY JOB_DATE DESC, JOB_CATEGORY ASC
    """
    df_job_details = pd.read_sql(job_info_query, _conn)
    return df_job_details


def viz_job_details(df_job_details):
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        #add filter for job roles
        job_category = sorted(set(df_job_details['JOB_ROLE'].dropna().unique()))
        select_job_category = st.selectbox("Filter by Role", ['All'] + job_category, key="filter_by_role_jobdetails")

    with col2:
        #add filter for states
        state_category = sorted(set(df_job_details['STATE'].dropna().unique()))
        select_state_category = st.selectbox("Filter by State", ['All'] + state_category, key="filter_by_state_jobdetails")

    with col3:
    #add filter for city
        city_category = sorted(set(df_job_details['CITY'].dropna().unique()))
        select_city_category = st.selectbox("Filter by City", ['All'] + city_category, key="filter_by_city_jobdetails")

    with col4:
    #add filter for orgs
        org_category = sorted(set(df_job_details['ORGANIZATION'].dropna().unique()))
        select_org_category = st.selectbox("Filter by Organization", ['All'] + org_category, key="filter_by_org_jobdetails")

    #define filter functions and allow them to work together
    def filter_by_selector(df):
        df_filtered = df.copy()
        if select_job_category != 'All':
            df_filtered = df_filtered[df_filtered['JOB_ROLE'] == select_job_category]
        if select_state_category != 'All':
            df_filtered = df_filtered[df_filtered['STATE'] == select_state_category]
        if select_city_category != 'All':
            df_filtered = df_filtered[df_filtered['CITY'] == select_city_category]
        if select_org_category != 'All':
            df_filtered = df_filtered[df_filtered['ORGANIZATION'] == select_org_category]
        return df_filtered

    filtered_job_table = filter_by_selector(df_job_details)

    #add links for job post
    if not filtered_job_table.empty:
        show_df = filtered_job_table.copy()

        #get the column index for Linkedin_postlink
        columns = show_df.columns.tolist()
        idx= columns.index("LINKEDIN_POST_LINK")
        show_df.insert(idx, "Job Link", show_df["LINKEDIN_POST_LINK"])
        show_df = show_df.drop(columns=["LINKEDIN_POST_LINK"])

        st.data_editor(
            show_df,
            column_config={
                "Job Link": st.column_config.LinkColumn("Job Link")
            },
            hide_index=True,
            height=600
        )
    else:
        st.info('No data for the filter(s) you have applied.')




def main():
    conn = connect_to_snowflake()
    df_dates = job_dates(conn)
    title(df_dates)
    df_job_total = query_job_total(conn)
    viz_job_data_summary(df_job_total)
    df_wow_trend = query_wow_trend(conn)
    viz_wow_trend(df_wow_trend)
    df_daily_jobs = query_daily_job_data(conn)
    viz_daily_job_data(df_daily_jobs)
    df_salary = query_salary_data(conn)
    viz_job_salary(df_salary)
    df_top_skills = query_top_skills_data(conn)
    viz_top_job_skills(df_top_skills)
    dfs_region_org = query_job_by_region_org(conn)
    viz_job_by_region_org(dfs_region_org)
    df_job_details = query_job_details(conn)
    viz_job_details(df_job_details)

if __name__ == "__main__":
    main()
