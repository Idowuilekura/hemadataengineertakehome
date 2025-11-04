# Hema ETL Technical Assessment

This repository contains my solution to the **Hema Data Engineer technical assessment**.  
It includes a PySpark-based ETL pipeline and an AWS design for securely sharing processed data between accounts.

---

## 📘 Overview

The goal is to build a realistic ETL pipeline for a retail sales dataset using **PySpark** and solid **data engineering practices**.  
The dataset is based on [Kaggle: Sales Forecasting](https://www.kaggle.com/datasets/rohitsahoo/sales-forecasting).

The pipeline ingests raw sales data, applies business transformations, and writes results into **Bronze**, **Silver**, and **Gold** layers following the **medallion architecture** pattern.

---

## 🧱 Project Structure

```
hema-etl-assessment/
  ├── src/
  │   ├── bronze_ingestion.py
  │   ├── silver_transformations.py
  │   ├── gold_sales_customers.py
  │   └── utils/
  │       ├── bronze_utils.py
  │       ├── silver_utils.py
  │       ├── gold_utils.py
  │       └── general_utils.py
  ├── data/
  │   ├── bronze/
  │   ├── silver/
  │   └── gold/
  ├── run_etl.py
  ├── requirements.txt
  ├── docker-compose.yaml
  └── README.md
```

---

## ⚙️ Pipeline Overview

### 1. Bronze Layer
- Reads the raw Kaggle CSV dataset.  
- Normalizes column names to `snake_case`.  
- Adds metadata columns:
  - `file_path`
  - `execution_datetime`
- Stores data as **Parquet**, partitioned by **order_date (year, month, day)**.

### 2. Silver Layer
- Cleans and standardizes data:
  - Type casting  
  - Date parsing  
  - Deduplication  
- Writes processed data to the silver layer in Parquet format.

### 3. Gold Layer
- Produces two curated datasets:
  - **Sales**
    - `order_id`, `order_date`, `shipment_date`, `shipment_mode`, `city`
  - **Customers**
    - `customer_id`, `first_name`, `last_name`, `segment`, `country`
    - Aggregations:
      - Orders in the past 1, 6, and 12 months (as of 2018-12-30)
      - Total lifetime orders
- The customer dataset is **fully refreshed** on each run.

### 4. Logging and Metadata
Each ETL step includes structured logging and execution metadata to support traceability and reproducibility.

---

## 🧪 Running the Pipeline

### Prerequisites
- Python 3.9+
- PySpark
- Docker and Docker Compose

### Setup

```bash
git clone https://github.com/idowuilekura/hemadataengineertakehome.git
cd hemadataengineertakehome

# Adjust the Docker volume path to point to your local project folder(this allows your files to be accessible to the container)
docker compose up -d

# Open a shell inside the running container
docker exec -it dockgispycont /bin/bash

# Install dependencies
pip install -r requirements.txt . This will download the additional modules.

Next, you need to get kaggle json to download the data. 


```
### Set Up Kaggle API Credentials

To download data from Kaggle, you need a Kaggle API token (`kaggle.json`).

Go to your [Kaggle account settings](https://www.kaggle.com/account).

Scroll down to the **API** section and click **“Create New API Token.”**  
This will download a file named `kaggle.json` containing your username and API key.

Example content:

```bash
{
  "username": "your_kaggle_username",
  "key": "your_long_api_key"
}

Move the file to the correct directory:
mkdir -p ~/.kaggle
mv /path/to/kaggle.json ~/.kaggle/
chmod 600 ~/.kaggle/kaggle.json

### Run

You can execute each layer individually:
```bash
python src/bronze_ingestion.py
python src/silver_transformations.py
python src/gold_sales_customers.py
```

Or run the full pipeline at once:
```bash
python src/run_etl.py
```

All outputs are written as Parquet files under the `data/` directory.

---

## ☁️ AWS Design (Part 2)

The second part of the assessment describes how this ETL pipeline would run in a **multi-account AWS environment**.

### Account Structure
- **Account A – Data Platform**
  - Hosts the data lake, ETL jobs, and Glue Catalog.
- **Account B – Analytics**
  - Used by analysts to query curated (Gold) datasets.

### Data Storage
All data is stored in **Amazon S3** in Account A:
- Organized into `bronze/`, `silver/`, and `gold/` prefixes.
- Partitioned by `year`, `month`, and `day`.
- Stored in Parquet format for efficiency.

### Cross-Account Access
- Account A’s S3 bucket policy grants **read-only access** to a specific **IAM role** in Account B.  
- The IAM role in Account B assumes permissions to read only the `gold/` layer paths.
- Access is policy-based; no Lake Formation or replication is used.

### Catalog and Querying
- **AWS Glue** in Account A maintains the metadata catalog (schemas for Gold tables).  
- The catalog is **shared with Account B** using **AWS Resource Access Manager (RAM)**.  
- A **Glue Crawler** automatically detects schema changes and updates the catalog.  
- Analysts in Account B use **Athena** to query the shared data directly.

### Security and Monitoring
- IAM policies restrict access to Gold-layer prefixes only.  
- All access and queries are logged through **AWS CloudTrail** and **CloudWatch**.

### Architecture Diagram

<img width="1522" height="792" alt="image" src="https://github.com/user-attachments/assets/dc2f841c-485c-4455-969b-6d438d20dfa3" />



---

## 🧹 Notes
- The pipeline automatically adjusts to new columns in the source data.  
- Each component is modular, making it easy to maintain and extend.  
- Logs and metadata provide clear visibility into each ETL stage.

## Future Works
- Run end-to-en test on the pipeline
- Add CI/CD with Github Actions
- Extend the pipeline with Airflow for Orchestrating 
