# HDB Resale Price Pipeline

An automated data pipeline that ingests, transforms, and summarises Singapore HDB resale flat transaction data from the official data.gov.sg API.

Built as a portfolio project to demonstrate data engineering skills relevant to the Singapore market.

## Architecture

data.gov.sg API → Apache Airflow (orchestration) → PostgreSQL (hdb_pipeline database)

## Pipeline Overview

The pipeline runs daily and consists of 3 tasks:

## Pipeline Overview

The pipeline runs daily and consists of 4 tasks:

1. **fetch_hdb_data** — Calls the data.gov.sg API and retrieves up to 10,000 HDB resale transactions, paginating through results in batches of 1,000
2. **validate_data** — Checks record count, required columns, and null price percentage before allowing processing to continue
3. **transform_data** — Cleans column names, converts data types, and adds a derived `price_per_sqm` column. Writes cleaned records to PostgreSQL
4. **save_data** — Aggregates results by town, computing average price, median price, and total transaction count. Writes summary to PostgreSQL

## Tech Stack

- **Apache Airflow 2.11.2** — Pipeline orchestration and scheduling
- **Docker + Docker Compose** — Containerised local environment
- **Python** — Pipeline logic
- **pandas** — Data transformation
- **data.gov.sg API** — Official Singapore government open data source

## Dataset

- **Source:** [HDB Resale Flat Prices — data.gov.sg](https://data.gov.sg/datasets/d_8b84c4ee58e3cfc0ece0d773c8ca6abc/view)
- **Coverage:** January 2017 to present
- **Updated:** Monthly

## Setup

### Prerequisites
- Docker Desktop
- Git

### Run locally

```bash
# Clone the repo
git clone https://github.com/JunKangN/hdb-pipeline.git
cd hdb-pipeline

# Set Airflow UID
echo "AIRFLOW_UID=50000" > .env

# Initialise and start Airflow
docker compose up airflow-init
docker compose up
```

Open `http://localhost:8080` — login with `airflow / airflow`

Enable and trigger the `hdb_resale_pipeline` DAG.

## Output

Two PostgreSQL tables in the `hdb_pipeline` database:

| Table | Description |
|---|---|
| `hdb_resale_transactions` | Cleaned transaction records with price_per_sqm column |
| `hdb_price_summary` | Average and median resale price by town, sorted by price |

## Sample Output

| Town | Avg Price | Median Price | Total Transactions |
|---|---|---|---|
| BISHAN | $650,000 | $630,000 | 245 |
| QUEENSTOWN | $620,000 | $600,000 | 312 |
| BUKIT TIMAH | $610,000 | $595,000 | 89 |

## What's Next

- [ ] Store results in PostgreSQL instead of CSV
- [ ] Add dbt models for further transformation
- [ ] Deploy to AWS (S3 + MWAA)
- [ ] Add data quality checks with Great Expectations
