import requests
import pandas as pd
from sqlalchemy import create_engine
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("etl_pipeline.log"),    #write logs
        logging.StreamHandler()                     #print logs to console
    ]
)

def extract():
    url = "https://jsonplaceholder.typicode.com/posts"

    logging.info("Starting data extraction from API....")
    try:
        response = requests.get(url)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        logging.error(f"API request failed: {e}")

    data = response.json()
    df = pd.DataFrame(data)
    logging.info(f"Extracted {len(df)} rows")

    return df

    
def transform(df):

    if df.empty:
        logging.warning("Dataframe received is empty")
        return df
    
    logging.info("Start data transformation...")

    df = df.rename(columns={
        "userId": "user_id",
        "id": "post_id"
    })
    df["title_length"] = df["title"].apply(len)     #create new column

    logging.info("Data transformation completed")
    return df

def load(df):
    if df.empty:
        logging.warning("Dataframe received is empty. Loading skipped")
        return 

    logging.info("Starting data load to PostgreSQL...")

    engine = create_engine("postgresql+psycopg2://postgres:password@localhost:5432/db")
    df.to_sql("complete_simple_pipeline", engine, if_exists="replace", index=False)

    logging.info(f"Loaded {len(df)} rows into the database")


def main():
    df = extract()
    df = transform(df)
    load(df)
    logging.info("\nPipeline executed successfully :]\n")

if __name__ == "__main__":
    main()