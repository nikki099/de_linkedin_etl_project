import pandas as pd
print("pandas version", pd.__version__)
import sys
print("Python executable:", sys.executable)

from pandas import json_normalize
import requests
import json
from dotenv import load_dotenv
import os
import urllib.parse
from googletrans import Translator
import re
from sqlalchemy import create_engine, text
from snowflake.connector import connect
from snowflake.connector.pandas_tools import write_pandas
import logging
import asyncio
import numpy as np



# --- 配置 logging ---
logging.basicConfig(
    format="%(asctime)s %(levelname)s: %(message)s",
    level=logging.INFO
)

# --- 加载环境变量 ---
load_dotenv()
rapidapi_key = os.getenv('RAPIDAPI_KEY')
rapidapi_host = "linkedin-job-search-api.p.rapidapi.com"
snowflake_password = os.getenv('SNOWFLAKE_PASSWORD')
snowflake_user = os.getenv('SNOWFLAKE_USER', 'NIKKILW2025')
snowflake_account = os.getenv('SNOWFLAKE_ACCOUNT', 'gbszkwp-by30611')

# --- Step 1：抓取LinkedIn数据 ---
def extract_linkedin_job_data():
    logging.info("Start extracting LinkedIn jobs from API...")
    headers = {
        'x-rapidapi-key': rapidapi_key,
        'x-rapidapi-host': rapidapi_host
    }
    location = "Australia"
    limit = 100
    offset = 0
    titles = ["Data Engineer", "Data Scientist", "Data Analyst"]
    df_daily_all = pd.DataFrame()

    for title_filter in titles:
        title_encoded = urllib.parse.quote(title_filter)
        location_encoded = urllib.parse.quote(location)
        base_url = f"/active-jb-24h?limit={limit}&offset={offset}&title_filter={title_encoded}&location_filter={location_encoded}"
        url = f"https://{rapidapi_host}{base_url}"

        try:
            response = requests.get(url, headers=headers)
            if response.status_code == 200:
                data = response.json()
                df_daily = json_normalize(data)
                df_daily['job_category'] = title_filter
                logging.info("[%s] Got %d rows.", title_filter, df_daily.shape[0])
                df_daily_all = pd.concat([df_daily_all, df_daily], ignore_index=True)
            else:
                logging.warning("API error [%s]: %d", title_filter, response.status_code)
        except Exception as e:
            logging.error("Exception during API call [%s]: %s", title_filter, str(e))
    logging.info("Finished extracting LinkedIn jobs. Total: %d", df_daily_all.shape[0])
    return df_daily_all

# --- Step 2：只保留目标职位 ---
def get_clean_data_jobs(df_daily_all):
    pattern = re.compile(r'\bData Engineer\b|\bData Scientist\b|\bData Analyst\b', re.IGNORECASE)
    df_clean = df_daily_all[df_daily_all['title'].str.contains(pattern, na=False)]
    logging.info("Cleaned job titles, remaining: %d", df_clean.shape[0])
    return df_clean

# Selcted the needed columns
def update_columns(df):
    df.columns = df.columns.str.upper()
    expected_columns = ['ID', 'DATE_POSTED', 'DATE_CREATED', 'TITLE', 'JOB_CATEGORY',
       'ORGANIZATION', 'ORGANIZATION_URL', 'DATE_VALIDTHROUGH', 'LOCATIONS_RAW',
       'LOCATION_TYPE', 'LOCATION_REQUIREMENTS_RAW', 'EMPLOYMENT_TYPE', 'URL',
       'SOURCE_TYPE', 'SOURCE', 'SOURCE_DOMAIN', 'ORGANIZATION_LOGO',
       'CITIES_DERIVED', 'REGIONS_DERIVED', 'COUNTRIES_DERIVED',
       'LOCATIONS_DERIVED', 'TIMEZONES_DERIVED', 'LATS_DERIVED',
       'LNGS_DERIVED', 'REMOTE_DERIVED', 'RECRUITER_NAME', 'RECRUITER_TITLE',
       'RECRUITER_URL', 'LINKEDIN_ORG_EMPLOYEES', 'LINKEDIN_ORG_URL',
       'LINKEDIN_ORG_SIZE', 'LINKEDIN_ORG_SLOGAN', 'LINKEDIN_ORG_INDUSTRY',
       'LINKEDIN_ORG_FOLLOWERS', 'LINKEDIN_ORG_HEADQUARTERS',
       'LINKEDIN_ORG_TYPE', 'LINKEDIN_ORG_FOUNDEDDATE',
       'LINKEDIN_ORG_SPECIALTIES', 'LINKEDIN_ORG_LOCATIONS',
       'LINKEDIN_ORG_DESCRIPTION', 'LINKEDIN_ORG_RECRUITMENT_AGENCY_DERIVED',
       'SENIORITY', 'DIRECTAPPLY', 'LINKEDIN_ORG_SLUG']

    # 记录缺失的预期列（但不删除任何现有列）
    missing = set(expected_columns) - set(df.columns)
    if missing:
        logging.warning("Expected columns missing in source data: %s", missing)

    logging.info("Final column count after update: %d", len(df.columns))
    df = df[expected_columns]
    return df

# --- Step 3：字段处理和提取 ---

def extract_job_date(df):
    df['JOB_DATE'] = pd.to_datetime(df['DATE_CREATED']).dt.date
    return df

def extract_city(locations_raw):
    text = str(locations_raw)
    city_pattern = r"'addressLocality':\s*'(.*?)',\s'addressRegion':"
    match = re.search(city_pattern, text)
    if match:
        city = match.group(1)
        if 'sidney' in city.lower() or 'sídney' in city.lower() or '悉尼' in city.lower():
            return "Sydney"
        return city
    else:
        return None

def extract_state(locations_raw):
    text = str(locations_raw)
    state_pattern = r"'addressRegion':\s*'(.*?)',\s'streetAddress'"
    match = re.search(state_pattern, text)
    if match:
        state = match.group(1).replace("'", "").strip()
        return state
    else:
        return None

def extract_employment_type(df):
    df['EMPLOYMENT_TYPE'] = (
        df['EMPLOYMENT_TYPE']
        .astype(str)
        .str.replace(r"[\[\]']", '', regex=True)
        .str.strip()
    )
    return df

def extract_employee_size(df):
    df['ORG_SIZE'] = (
        df['LINKEDIN_ORG_SIZE']
        .astype(str)
        .str.replace(r"employees", '', regex=True)
        .str.strip()
    )
    return df


#Only keep the relevant columns
def extract_relevant_columns(df):
    relevant_columns = ['ID', 'TITLE', 'JOB_CATEGORY',
       'JOB_DATE', 'CITY', 'STATE', 'EMPLOYMENT_TYPE' ,
       'ORGANIZATION', 'ORGANIZATION_URL', 'URL',
       'SOURCE_TYPE', 'SOURCE', 'SOURCE_DOMAIN',
       'ORGANIZATION_LOGO', 'REMOTE_DERIVED', 'RECRUITER_NAME',
       'RECRUITER_TITLE', 'RECRUITER_URL', 'LINKEDIN_ORG_URL',
       'ORG_SIZE', 'LINKEDIN_ORG_INDUSTRY', 'LINKEDIN_ORG_HEADQUARTERS',
       'LINKEDIN_ORG_TYPE', 'LINKEDIN_ORG_FOUNDEDDATE',
       'LINKEDIN_ORG_SPECIALTIES', 'LINKEDIN_ORG_LOCATIONS',
       'LINKEDIN_ORG_DESCRIPTION','LINKEDIN_ORG_RECRUITMENT_AGENCY_DERIVED',
       'SENIORITY', 'DIRECTAPPLY', 'LINKEDIN_ORG_SLUG' ]
    df = df[relevant_columns]
    logging.info("final data frame with only relevent columns %d", len(df.columns))
    return df




# --- Step 4：连接到 Snowflake ---
def connect_to_snowflake():
    try:
        conn = connect(
            user=snowflake_user,
            password=snowflake_password,
            account=snowflake_account,
            warehouse="SNOWFLAKE_LEARNING_WH",
            database="linkedin_db",
            schema="linkedin_raw"
        )
        logging.info("Connected to Snowflake successfully.")
        return conn
    except Exception as e:
        logging.error("Failed to connect to Snowflake: %s", str(e))
        return None

def query_existing_job_data(conn):
    query = """
        SELECT * FROM LINKEDIN_JOB_API_CLEANED_DATA
        WHERE (
            lower(TITLE) LIKE '%data engineer%'
            OR lower(TITLE) LIKE '%data scientist%'
            OR lower(TITLE) LIKE '%data analyst%'
        )
    """
    df = pd.read_sql(query, conn)
    logging.info("Queried existing job data from Snowflake, rows: %d", df.shape[0])
    return df


#Check the Job ID from df and only keep those new jobs based on the Job IDs
def keep_new_jobs(df_daily_all: pd.DataFrame, existing_df: pd.DataFrame) -> pd.DataFrame:
    logging.info("Starting filtering new jobs based on job IDs.")

    existing_job_ids = existing_df['ID'].unique().tolist()
    logging.info("Number of existing job IDs: %d", len(existing_job_ids))

    df_new_jobs = df_daily_all[~df_daily_all['ID'].isin(existing_job_ids)].reset_index(drop=True)
    logging.info("Number of new jobs identified: %d", df_new_jobs.shape[0])

    #convert empty strings to NaN
    df_new_jobs.replace('', np.nan, inplace=True)
    return df_new_jobs




# --- Step 5：对新增数据进行翻译 ---
async def translate_text(translator, text, target_language='en'):
    try:
        if not text or pd.isna(text):
            return 'NA'
        result = await translator.translate(str(text), dest=target_language)
        return result.text
    except Exception as e:
        logging.warning(f"Error translating text: {e} (Text: {text})")
        return text

async def translate_column(df, col, target_language='en'):
    translator = Translator()
    unique_values = df[col].dropna().unique()
    tasks = {val: translate_text(translator, val, target_language) for val in unique_values}
    translation_map = {}
    for val, task in tasks.items():
        translation_map[val] = await task
    df[col] = df[col].map(translation_map).fillna('NA')

async def translate_columns(df, columns, target_language='en'):
    for col in columns:
        await translate_column(df, col, target_language)
    logging.info(f"Translated columns: {columns}")
    return df

def sync_translate_columns(df, columns, target_language='en'):
    """同步调用异步的列翻译函数，兼容普通脚本环境"""
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            # 如果已有事件循环在运行，使用 nest_asyncio
            import nest_asyncio
            nest_asyncio.apply()
        return loop.run_until_complete(translate_columns(df, columns, target_language))
    except RuntimeError as e:
        logging.error(f"Event loop error: {e}, creating new event loop.")
        # 创建新的事件循环，保障兼容性
        new_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(new_loop)
        return new_loop.run_until_complete(translate_columns(df, columns, target_language))



# Step 6：加载数据到 Snowflake ---
# 增强 load_to_snowflake 函数（添加动态列对齐）
def load_to_snowflake(df, table_name="LINKEDIN_JOB_API_CLEANED_DATA"):
    # Snowflake connection parameters
    conn_params = {
        'user': snowflake_user,
        'password': snowflake_password,
        'account': snowflake_account,
        'warehouse': 'SNOWFLAKE_LEARNING_WH',
        'database': 'linkedin_db',
        'schema': 'linkedin_raw'
    }

    # Create Snowflake connection
    conn = connect(**conn_params)

    # Ensure column names are uppercase to match Snowflake
    df.columns = df.columns.str.upper()

    # Check if the table exists
    try:
        with conn.cursor() as cursor:
            cursor.execute(f"DESC TABLE {table_name}")
            logging.info("Table %s exists.", table_name)
    except Exception as e:
        logging.error("Table %s does not exist or cannot be accessed: %s", table_name, e)
        conn.close()
        return

    # Load DataFrame to Snowflake
    try:
        # Append the DataFrame to the existing table
        write_pandas(conn, df, table_name, auto_create_table=False)
        logging.info("Successfully loaded %d rows to Snowflake", len(df))
    except Exception as e:
        logging.error("An error occurred while loading data to Snowflake: %s", e)
    finally:
        conn.close()  # Ensure the connection is closed after loading



# --- Main Flow ---
def main():
    df_daily_all = extract_linkedin_job_data()
    if df_daily_all.empty:
        logging.error("No job data extracted; aborting script.")
        return

    df_daily_all = get_clean_data_jobs(df_daily_all)
    df_daily_all = update_columns(df_daily_all)
    df_daily_all = extract_job_date(df_daily_all)
    df_daily_all['CITY'] = df_daily_all['LOCATIONS_RAW'].apply(extract_city)
    df_daily_all['STATE'] = df_daily_all['LOCATIONS_RAW'].apply(extract_state)
    df_daily_all = extract_employment_type(df_daily_all)
    df_daily_all = extract_employee_size(df_daily_all)
    df_daily_all = extract_relevant_columns(df_daily_all)

    conn = connect_to_snowflake()
    if conn is None:
        logging.error("No connection to Snowflake; aborting script.")
        return
    df_existing = query_existing_job_data(conn)
    df_new_jobs=keep_new_jobs(df_daily_all, df_existing)
    print(f'df_new_jobs shape is {df_new_jobs.shape}')
    logging.info("New jobs found: %d", df_new_jobs.shape[0])


    if df_new_jobs.empty:
        logging.info("No new jobs to load; script finished.")
        return

    #------------- CALL TRANSLATION ASYNC -----------
    translate_cols = ['CITY', 'STATE', 'ORGANIZATION', 'SENIORITY']
    df_new_jobs = sync_translate_columns(df_new_jobs, translate_cols)
    #-------------------------------------------------

    load_to_snowflake(df_new_jobs)

if __name__ == "__main__":
    main()
