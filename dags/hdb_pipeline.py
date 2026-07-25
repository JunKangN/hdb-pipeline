from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
import pandas as pd
import os
import time

RAW_DATA_PATH = "/opt/airflow/dags/data/hdb_raw.csv"
CLEANED_DATA_PATH = "/opt/airflow/dags/data/hdb_cleaned.csv"
DATASET_ID = "d_8b84c4ee58e3cfc0ece0d773c8ca6abc"



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
        
        time.sleep(1)  # 1 second pause between requests
        
        if offset >= 10000:
            break
    
    os.makedirs(os.path.dirname(RAW_DATA_PATH), exist_ok=True)
    df = pd.DataFrame(all_records)
    df.to_csv(RAW_DATA_PATH, index=False)
    print(f"Saved {len(df)} records to {RAW_DATA_PATH}")

def transform_data():
    print("Transforming HDB data...")
    
    df = pd.read_csv(RAW_DATA_PATH)
    
    # Clean column names
    df.columns = df.columns.str.lower().str.replace(" ", "_")
    
    # Convert price to numeric
    df["resale_price"] = pd.to_numeric(df["resale_price"], errors="coerce")
    
    # Convert floor area to numeric
    df["floor_area_sqm"] = pd.to_numeric(df["floor_area_sqm"], errors="coerce")
    
    # Add price per sqm column
    df["price_per_sqm"] = (df["resale_price"] / df["floor_area_sqm"]).round(2)
    
    # Drop rows with missing prices
    df = df.dropna(subset=["resale_price"])
    
    df.to_csv(CLEANED_DATA_PATH, index=False)
    print(f"Transformed data saved — {len(df)} clean records")

def save_data():
    print("Summarising cleaned HDB data...")
    
    df = pd.read_csv(CLEANED_DATA_PATH)
    
    # Average price by town
    summary = df.groupby("town")["resale_price"].agg(
        avg_price="mean",
        median_price="median",
        total_transactions="count"
    ).round(2).reset_index()
    
    summary = summary.sort_values("avg_price", ascending=False)
    
    print("\n--- Top 5 Most Expensive Towns ---")
    print(summary.head())
    
    summary_path = "/opt/airflow/dags/data/hdb_summary.csv"
    summary.to_csv(summary_path, index=False)
    print(f"Summary saved to {summary_path}")

# DAG definition
with DAG(
    dag_id="hdb_resale_pipeline",
    start_date=datetime(2025, 7, 1),
    schedule="@daily",
    catchup=False,
    tags=["hdb", "singapore"]
) as dag:

# The tasks
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

    fetch >> transform >> save