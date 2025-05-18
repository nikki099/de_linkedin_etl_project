import logging
import pandas as pd
from pandas import json_normalize
import requests
import os
import urllib.parse
from dotenv import load_dotenv

# 配置 logging
logging.basicConfig(
    format="%(asctime)s %(levelname)s: %(message)s",
    level=logging.INFO
)

load_dotenv()
rapidapi_key = os.getenv('RAPIDAPI_KEY')
rapidapi_host = "linkedin-job-search-api.p.rapidapi.com"

def extract_linkedin_job_data():
    logging.info("Start extracting LinkedIn jobs from API...")
    if not rapidapi_key:
        logging.error("RAPIDAPI_KEY not set. Please check your environment variables.")
        return pd.DataFrame()

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

        logging.info(f"Requesting jobs for title: {title_filter} from {url}")

        try:
            response = requests.get(url, headers=headers)
            if response.status_code == 200:
                data = response.json()
                df_daily = json_normalize(data)
                df_daily['job_category'] = title_filter
                logging.info("[%s] Got %d rows.", title_filter, df_daily.shape[0])
                df_daily_all = pd.concat([df_daily_all, df_daily], ignore_index=True)
            else:
                logging.warning("API error [%s]: %d, content: %s", title_filter, response.status_code, response.text)
        except Exception as e:
            logging.error("Exception during API call [%s]: %s", title_filter, str(e))
    logging.info("Finished extracting LinkedIn jobs. Total rows: %d", df_daily_all.shape[0])
    return df_daily_all

def main():
    logging.info(f"Using RAPIDAPI_KEY: {rapidapi_key[:5]}...")  # 部分打印，确认有值
    df = extract_linkedin_job_data()
    if df.empty:
        logging.error("No job data extracted; aborting script.")
    else:
        logging.info(f"Extracted {df.shape[0]} rows. Sample:")
        print(df.head())

if __name__ == "__main__":
    main()
