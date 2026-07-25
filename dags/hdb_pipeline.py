from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
import pandas as pd
import os
import time
from sqlalchemy import create_engine

RAW_DATA_PATH = "/opt/airflow/dags/data/hdb_raw.csv"
DATASET_ID = "d_8b84c4ee58e3cfc0ece0d773c8ca6abc"
DB_CONN = "postgresql+psycopg2://airflow:airflow@postgres/hdb_pipeline"

def fetch_hdb_data():
    print("Fetching HDB resale data from data.gov.sg...")
    
    all_records = []
    limit = 1000
    offset = 0
    
    while True:
        url = f"https://data.gov.sg/api/action/datastore_search?resource_id={DATASET_ID}&limit={limit}&offset={offset}"
        response = requests.get(url)
        
        if response.status_code != 200:
            print(f"API error: {response.status_code} — stopping fetch")
            break
            
        data = response.json()
        
        if not data.get("success"):
            print(f"API returned failure: {data}")
            break
            
        records = data["result"]["records"]
        
        if not records:
            break
            
        all_records.extend(records)
        offset += limit
        print(f"Fetched {len(all_records)} records so far...")
        
        time.sleep(1)
        
        if offset >= 10000:
            break
    
    os.makedirs(os.path.dirname(RAW_DATA_PATH), exist_ok=True)
    df = pd.DataFrame(all_records)
    df.to_csv(RAW_DATA_PATH, index=False)
    print(f"Saved {len(df)} records to {RAW_DATA_PATH}")

def validate_data():
    '''
    What each check does:

    Check 1 — if the API returns fewer than 100 records, something is wrong — abort
    Check 2 — if expected columns are missing, the API structure changed — abort
    Check 3 — if more than 10% of prices are null, data quality is too poor to process — abort

    If any check fails, Airflow marks the task as failed and stops the pipeline — transform and save never run, so good data won't be overwritten with bad data.
    '''
    print("Validating fetched data...")
    
    if not os.path.exists(RAW_DATA_PATH):
        raise FileNotFoundError(f"Raw data file not found at {RAW_DATA_PATH}")
    
    df = pd.read_csv(RAW_DATA_PATH)
    
    # Check 1 — minimum record count
    if len(df) < 100:
        raise ValueError(f"Too few records: {len(df)}. Expected at least 100.")
    
    # Check 2 — required columns exist
    required_columns = ["month", "town", "flat_type", "resale_price", "floor_area_sqm"]
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")
    
    # Check 3 — no completely empty resale_price column
    null_price_pct = df["resale_price"].isnull().mean()
    if null_price_pct > 0.1:
        raise ValueError(f"Too many null prices: {null_price_pct:.1%}")
    
    print(f"Validation passed — {len(df)} records, all required columns present")

def transform_data():
    print("Transforming HDB data...")
    
    df = pd.read_csv(RAW_DATA_PATH)
    
    df.columns = df.columns.str.lower().str.replace(" ", "_")
    df["resale_price"] = pd.to_numeric(df["resale_price"], errors="coerce")
    df["floor_area_sqm"] = pd.to_numeric(df["floor_area_sqm"], errors="coerce")
    df["price_per_sqm"] = (df["resale_price"] / df["floor_area_sqm"]).round(2)
    df = df.dropna(subset=["resale_price"])
    
    engine = create_engine(DB_CONN)
    df.to_sql(
        "hdb_resale_transactions",
        engine,
        if_exists="replace",
        index=False
    )
    print(f"Written {len(df)} records to hdb_resale_transactions table")

def save_data():
    print("Summarising HDB data...")
    
    engine = create_engine(DB_CONN)
    df = pd.read_sql("SELECT * FROM hdb_resale_transactions", engine)
    
    summary = df.groupby("town")["resale_price"].agg(
        avg_price="mean",
        median_price="median",
        total_transactions="count"
    ).round(2).reset_index()
    
    summary = summary.sort_values("avg_price", ascending=False)
    
    summary.to_sql(
        "hdb_price_summary",
        engine,
        if_exists="replace",
        index=False
    )
    
    print("\n--- Top 5 Most Expensive Towns ---")
    print(summary.head())
    print("Summary written to hdb_price_summary table")

with DAG(
    dag_id="hdb_resale_pipeline",
    start_date=datetime(2025, 7, 1),
    schedule="@daily",
    catchup=False,
    tags=["hdb", "singapore"]
) as dag:

    fetch = PythonOperator(
        task_id="fetch_hdb_data",
        python_callable=fetch_hdb_data
    )

    transform = PythonOperator(
        task_id="transform_data",
        python_callable=transform_data
    )

    save = PythonOperator(
        task_id="save_data",
        python_callable=save_data
    )

    validate = PythonOperator(
        task_id="validate_data",
        python_callable=validate_data
    )

    fetch >> validate >> transform >> save

    