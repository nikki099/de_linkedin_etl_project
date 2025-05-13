import pandas as pd
from pandas import json_normalize
import requests
import json
import snowflake.connector
from dotenv import load_dotenv
import os
import http.client
import urllib.parse
from googletrans import Translator
import string
import re
from sqlalchemy import create_engine
from sqlalchemy.sql import text
import logging
import asyncio


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
    df_daily_all.to_csv('linkedin_jobs_daily.csv', index=False)
    df_daily_all= pd.read_csv('linkedin_jobs_daily.csv')
    logging.info("Finished extracting LinkedIn jobs. Total: %d", df_daily_all.shape[0])
    return df_daily_all

# --- Step 2：只保留目标职位 ---
def get_clean_data_jobs(df_daily_all):
    pattern = re.compile(r'\bData Engineer\b|\bData Scientist\b|\bData Analyst\b', re.IGNORECASE)
    df_clean = df_daily_all[df_daily_all['title'].str.contains(pattern, na=False)]
    logging.info("Cleaned job titles, remaining: %d", df_clean.shape[0])
    return df_clean

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

def update_columns(df):
    df.columns = df.columns.str.upper()
    expected_columns = [
            'ID', 'DATE_POSTED', 'DATE_CREATED', 'TITLE', 'JOB_CATEGORY',
            'ORGANIZATION', 'ORGANIZATION_URL', 'DATE_VALIDTHROUGH', 'LOCATIONS_RAW',
            'LOCATION_TYPE', 'LOCATION_REQUIREMENTS_RAW', 'EMPLOYMENT_TYPE', 'URL',
            'SOURCE_TYPE', 'SOURCE', 'SOURCE_DOMAIN', 'ORGANIZATION_LOGO',
            'CITIES_DERIVED', 'REGIONS_DERIVED', 'COUNTRIES_DERIVED', 'LOCATIONS_DERIVED',
            'TIMEZONES_DERIVED', 'LATS_DERIVED', 'LNGS_DERIVED', 'REMOTE_DERIVED',
            'RECRUITER_NAME', 'RECRUITER_TITLE', 'RECRUITER_URL', 'LINKEDIN_ORG_EMPLOYEES',
            'LINKEDIN_ORG_URL', 'LINKEDIN_ORG_SIZE', 'LINKEDIN_ORG_SLOGAN', 'LINKEDIN_ORG_INDUSTRY',
            'LINKEDIN_ORG_FOLLOWERS', 'LINKEDIN_ORG_HEADQUARTERS', 'LINKEDIN_ORG_TYPE', 'LINKEDIN_ORG_FOUNDEDDATE',
            'LINKEDIN_ORG_SPECIALTIES', 'LINKEDIN_ORG_LOCATIONS', 'LINKEDIN_ORG_DESCRIPTION',
            'LINKEDIN_ORG_RECRUITMENT_AGENCY_DERIVED', 'SENIORITY', 'DIRECTAPPLY', 'LINKEDIN_ORG_SLUG'
        ]

    # 记录缺失的预期列（但不删除任何现有列）
    missing = set(expected_columns) - set(df.columns)
    if missing:
        logging.warning("Expected columns missing in source data: %s", missing)

    logging.info("Final column count after update: %d", len(df.columns))
    return df

# --- Step 4：连接到 Snowflake ---
def connect_to_snowflake():
    try:
        conn = snowflake.connector.connect(
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

# --- Step 5：对新增数据进行翻译 ---
async def translate_text(translator, text, target_language='en'):
    """Translate a single text value asynchronously."""
    try:
        if not text or pd.isna(text):
            return 'NA'
        result = await translator.translate(str(text), dest=target_language)
        return result.text
    except Exception as e:
        logging.warning("Error translating text: %s (Text: %s)", str(e), text)
        return text

async def translate_columns(df, columns, target_language='en'):
    """
    Asynchronously translate specified columns in a DataFrame to a target language ("en" by default).
    """
    translator = Translator()
    for col in columns:
        unique_values = df[col].dropna().unique()
        # Asynchronous batch translation for faster performance
        tasks = {val: translate_text(translator, val, target_language) for val in unique_values}
        translation_map = {}
        for val, task in tasks.items():
            translation_map[val] = await task
        df[col] = df[col].map(translation_map).fillna('NA')
    logging.info("Translated columns for: %s", columns)
    return df

# Step 6：加载数据到 Snowflake ---
# 增强 load_to_snowflake 函数（添加动态列对齐）
def load_to_snowflake(df, table_name="linkedin_job_api_cleaned_data"):
    engine = create_engine(
        'snowflake://{user}:{password}@{account}/{database}/{schema}?warehouse={warehouse}'.format(
            user=snowflake_user,
            password=snowflake_password,
            account=snowflake_account,
            warehouse="SNOWFLAKE_LEARNING_WH",
            database="linkedin_db",
            schema="linkedin_raw"
        )
    )

    # 动态获取目标表列
    with engine.connect() as conn:
        result = conn.execute(text(f"DESC TABLE {table_name}"))
        table_columns = [row[0].upper() for row in result]  # 取第一个字段（列名）

    # 列名大小写一致性处理
    df.columns = df.columns.str.upper()

    # 自动补全缺失列
    for col in table_columns:
        if col not in df.columns:
            df[col] = None  # 填充空值
            logging.info("Added missing column: %s", col)

    # 按目标表列顺序排序
    df = df[table_columns]

    # 数据加载
    df.to_sql(
        name=table_name,
        con=engine,
        if_exists='append',
        index=False
    )
    logging.info("Successfully loaded %d rows to Snowflake", len(df))

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

    conn = connect_to_snowflake()
    if conn is None:
        logging.error("No connection to Snowflake; aborting script.")
        return
    df_existing = query_existing_job_data(conn)
    new_job_ids = set(df_daily_all['ID']) - set(df_existing['ID'])

    df_new_jobs = df_daily_all[df_daily_all['ID'].isin(new_job_ids)].reset_index(drop=True)
    print(f'df_new_jobs shape is {df_new_jobs.shape}')
    logging.info("New jobs found: %d", df_new_jobs.shape[0])

    if df_new_jobs.empty:
        logging.info("No new jobs to load; script finished.")
        return

    # ------------- CALL TRANSLATION ASYNC -----------
    translate_cols = ['CITY', 'STATE', 'ORGANIZATION', 'SENIORITY']
    df_new_jobs = asyncio.run(translate_columns(df_new_jobs, translate_cols))
    # -------------------------------------------------

    print(df_new_jobs.shape)
    load_to_snowflake(df_new_jobs)

if __name__ == "__main__":
    main()
