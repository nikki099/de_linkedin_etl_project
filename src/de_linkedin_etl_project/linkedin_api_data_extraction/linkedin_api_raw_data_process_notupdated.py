import os
import re
import logging
import urllib.parse
import pandas as pd
from pandas import json_normalize
import requests
import json

from dotenv import load_dotenv

from googletrans import Translator
import snowflake.connector
from sqlalchemy import create_engine

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

translator = Translator()

def extract_linkedin_job_data(rapidapi_key, rapidapi_host):
    logger.info("Starting LinkedIn job data extraction from API (last 24h)...")
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

        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            data = response.json()
            df_daily = json_normalize(data)
            df_daily['JOB_CATEGORY'] = title_filter
            logger.info(f"Fetched '{title_filter}' jobs: {df_daily.shape[0]} rows")
            df_daily_all = pd.concat([df_daily_all, df_daily], ignore_index=True)
        else:
            logger.error(f"API error for '{title_filter}': HTTP {response.status_code}")
    # df_daily_all = pd.read_csv('linkedin_jobs_daily.csv')

    logger.info("LinkedIn job data extraction completed.")
    return df_daily_all

def get_clean_data_jobs(df_daily_all):
    logger.info("Filter down to related data jobs.")
    pattern=re.compile(r'\bData Engineer\b|\bData Scientist\b|\bData Analyst\b' , re.IGNORECASE)
    df_daily_all = df_daily_all[df_daily_all['TITLE'].str.contains(pattern)]
    logger.info("Kept DE, DS and DA roles only.")
    return df_daily_all


def update_columns(df):
    logger.info("Updating column names and selecting relevant columns ...")
    df.columns = df.columns.str.upper()
    columns_keep = [
        'ID', 'DATE_POSTED', 'DATE_CREATED', 'TITLE', 'JOB_CATEGORY',
        'ORGANIZATION', 'ORGANIZATION_URL', 'DATE_VALIDTHROUGH', 'LOCATIONS_RAW',
        'LOCATION_TYPE', 'LOCATION_REQUIREMENTS_RAW', 'EMPLOYMENT_TYPE', 'URL',
        'SOURCE_TYPE', 'SOURCE', 'SOURCE_DOMAIN', 'ORGANIZATION_LOGO',
        'CITIES_DERIVED', 'REGIONS_DERIVED', 'COUNTRIES_DERIVED',
        'LOCATIONS_DERIVED', 'TIMEZONES_DERIVED', 'LATS_DERIVED', 'LNGS_DERIVED',
        'REMOTE_DERIVED', 'RECRUITER_NAME', 'RECRUITER_TITLE', 'RECRUITER_URL',
        'LINKEDIN_ORG_EMPLOYEES', 'LINKEDIN_ORG_URL', 'LINKEDIN_ORG_SIZE',
        'LINKEDIN_ORG_SLOGAN', 'LINKEDIN_ORG_INDUSTRY', 'LINKEDIN_ORG_FOLLOWERS',
        'LINKEDIN_ORG_HEADQUARTERS', 'LINKEDIN_ORG_TYPE', 'LINKEDIN_ORG_FOUNDEDDATE',
        'LINKEDIN_ORG_SPECIALTIES', 'LINKEDIN_ORG_LOCATIONS', 'LINKEDIN_ORG_DESCRIPTION',
        'LINKEDIN_ORG_RECRUITMENT_AGENCY_DERIVED', 'SENIORITY', 'DIRECTAPPLY',
        'LINKEDIN_ORG_SLUG'
    ]
    selected_columns = [col for col in columns_keep if col in df.columns]
    df = df[selected_columns]
    logger.info("Updated and selected columns.")
    return df

def extract_job_date(df):
    logger.info("Extracting job date ...")
    df['JOB_DATE'] = pd.to_datetime(df['DATE_CREATED']).dt.date
    return df

def extract_city(text):
    if pd.isna(text):
        return
    text = str(text)
    city_pattern = r"'addressLocality':\s*'(.*)',\s'addressRegion':"
    match = re.search(city_pattern, text)
    if match:
        city = match.group(1)
        if 'sidney' in city.lower() or 'sídney' in city.lower() or '悉尼' in city.lower():
            return "Sydney"
        return city
    else:
        return None

def extract_state(text):
    text = str(text)
    state_pattern = r"'addressRegion':\s*(.*)',\s'streetAddress'"
    match = re.search(state_pattern, text)
    if match:
        state = match.group(1).replace("'", "").strip()
        return state
    else:
        return None

def extract_employment_type(df):
    logger.info("Normalizing EMPLOYMENT_TYPE ...")
    df['EMPLOYMENT_TYPE'] = (
        df['EMPLOYMENT_TYPE']
        .astype(str)
        .str.replace(r"[\[\]']", '', regex=True)
        .str.strip()
    )
    return df

def extract_employee_size(df):
    logger.info("Normalizing ORG_SIZE ...")
    df['ORG_SIZE'] = (
        df['LINKEDIN_ORG_SIZE']
        .astype(str)
        .str.replace(r"employees", '', regex=True)
        .str.strip()
    )
    return df

def connect_to_snowflake(user, password, account, warehouse, database, schema):
    logger.info("Connecting to Snowflake ...")
    try:
        conn = snowflake.connector.connect(
            user=user,
            password=password,
            account=account,
            warehouse=warehouse,
            database=database,
            schema=schema
        )
        logger.info("Connection to Snowflake established successfully.")
        return conn
    except Exception as e:
        logger.error(f"Error connecting to Snowflake: {e}")
        return None

def query_existing_job_data(conn, table_name='linkedin_job_api_cleaned_data'):
    logger.info(f"Querying raw API data from table: {table_name} ...")
    query = """
        SELECT * FROM LINKEDIN_JOB_API_CLEANED_DATA
        WHERE (
            lower(TITLE) LIKE '%data engineer%'
            OR lower(TITLE) LIKE '%data scientist%'
            OR lower(TITLE) LIKE '%data analyst%'
        )
    """
    df = pd.read_sql(query, conn)
    logger.info(f"Retrieved {df.shape[0]} rows from Snowflake.")
    return df

def drop_existing_jobs(df_new, df_existing):
    logger.info("Removing jobs that already exist in database ...")
    existing_job_ids = df_existing['ID'].unique().tolist()
    df_new_only = df_new[~(df_new['ID'].isin(existing_job_ids))].reset_index(drop=True)
    logger.info(f"{df_new_only.shape[0]} new jobs remain.")
    return df_new_only

def translate_text(text, target_language='en'):
    try:
        if not text or pd.isna(text) or str(text).strip().lower() in ['nan', 'na']:
            logger.warning("Invalid text for translation. Returning 'NA'.")
            return 'NA'
        translated_text = translator.translate(text, dest=target_language)
        logger.info(f"Translated '{text}' to '{translated_text.text}'")
        return translated_text.text
    except Exception as e:
        logger.error(f"Error translating text '{text}': {e}")
        return text


def translate_column(df, col, target_language='en'):
    #cache translation to avoid repeated requests
    unique_vals = df[col].dropna().unique()
    translation_map = {}
    for val in unique_vals:
        try:
            translation_map[val] = translate_text(val, target_language)
        except Exception as e:
            logger.error(f"Error translating: {val}. Error: {e}")
            translation_map[val] = val
    df[col] = df[col].map(translation_map).fillna(df[col])
    return df



def load_to_snowflake(df_new_jobs, user, password, account, warehouse, database, schema, table_name):

    engine = create_engine(
        f'snowflake://{user}:{password}@{account}/{database}/{schema}?warehouse={warehouse}',
        connect_args={'client_session_keep_alive': True}
    )

    df_new_jobs.to_sql(
        name=table_name,
        con=engine,
        if_exists='append', #append data
        index=False
    )

    print(f"Data loaded to Snowflake table {table_name} successfully.")



def main():
    # 1. Load environment variables
    load_dotenv()
    rapidapi_key = os.getenv('RAPIDAPI_KEY')
    rapidapi_host = "linkedin-job-search-api.p.rapidapi.com"
    snowflake_password = os.getenv('SNOWFLAKE_PASSWORD')
    user = "NIKKILW2025"
    account = "gbszkwp-by30611"
    warehouse = "SNOWFLAKE_LEARNING_WH"
    database = "linkedin_db"
    schema = "linkedin_raw"
    table_name = "linkedin_job_api_cleaned_data"

    # 2. Fetch & process new LinkedIn jobs from API
    df_daily_all = extract_linkedin_job_data(rapidapi_key, rapidapi_host)
    df_daily_all = update_columns(df_daily_all)
    df_daily_all.to_csv('linkedin_jobs_daily.csv', index=False)
    logger.info("Saved scraped data to linkedin_jobs_daily.csv.")

    # 3. Filter API Data to DE, DS, DA only
    df_daily_all = get_clean_data_jobs(df_daily_all)


    # 4. Extract fields
    df_daily_all = extract_job_date(df_daily_all)
    df_daily_all['CITY'] = df_daily_all['LOCATIONS_RAW'].apply(extract_city)
    df_daily_all['STATE'] = df_daily_all['LOCATIONS_RAW'].apply(extract_state)
    df_daily_all = extract_employment_type(df_daily_all)
    df_daily_all = extract_employee_size(df_daily_all)
    needed_cols = [
        'ID', 'TITLE', 'JOB_CATEGORY', 'JOB_DATE', 'CITY', 'STATE', 'EMPLOYMENT_TYPE',
        'ORGANIZATION', 'ORGANIZATION_URL', 'URL', 'SOURCE_TYPE', 'SOURCE',
        'SOURCE_DOMAIN', 'ORGANIZATION_LOGO', 'REMOTE_DERIVED', 'RECRUITER_NAME',
        'RECRUITER_TITLE', 'RECRUITER_URL', 'LINKEDIN_ORG_URL', 'ORG_SIZE',
        'LINKEDIN_ORG_INDUSTRY', 'LINKEDIN_ORG_HEADQUARTERS', 'LINKEDIN_ORG_TYPE',
        'LINKEDIN_ORG_FOUNDEDDATE', 'LINKEDIN_ORG_SPECIALTIES',
        'LINKEDIN_ORG_LOCATIONS', 'LINKEDIN_ORG_DESCRIPTION',
        'LINKEDIN_ORG_RECRUITMENT_AGENCY_DERIVED', 'SENIORITY', 'DIRECTAPPLY',
        'LINKEDIN_ORG_SLUG'
    ]
    df_daily_all = df_daily_all[needed_cols]

    # 5. Connect to Snowflake and query existing job data
    conn = connect_to_snowflake(user, snowflake_password, account, warehouse, database, schema)
    if conn is None:
        logger.error("Could not connect to Snowflake. Exiting pipeline.")
        return
    df_existing = query_existing_job_data(conn)
    conn.close()

    # 6. Identify and keep only new jobs
    df_new_jobs = drop_existing_jobs(df_daily_all, df_existing)

    # 7. Translate key fields to English
    logger.info("Translating CITY, STATE, ORGANIZATION, SENIORITY ...")
    for col in ['CITY', 'STATE', 'ORGANIZATION', 'SENIORITY']:
        df_new_jobs = translate_column(df_new_jobs, col, target_language='en')

    # 8. Load new jobs to Snowflake
    load_to_snowflake(
    df_new_jobs,
    user,
    snowflake_password,
    account,
    warehouse,
    database,
    schema,
    table_name
    )
    logger.info("Pipeline finished successfully.")

if __name__ == "__main__":
    main()
