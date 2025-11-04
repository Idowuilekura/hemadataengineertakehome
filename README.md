# Hema ETL Technical Assessment

This repo contains my solution to the Hema Data Engineer technical assessment.  
It includes a PySpark-based ETL pipeline and an AWS design for securely sharing processed data between accounts.

---

## 📘 Overview

The goal was to build a small but realistic ETL pipeline for a retail sales dataset using **PySpark** and **data engineering best practices**.  
The dataset comes from [Kaggle: Sales Forecasting](https://www.kaggle.com/datasets/rohitsahoo/sales-forecasting).

The pipeline ingests raw sales data, applies transformations and business rules, and writes the results into **Bronze**, **Silver**, and **Gold** layers of a data lake following the **medallion architecture** pattern.

---

## 🧱 Project Structure

```
base_folder
  hema_etl/
  ├── config/
  │   └── spark_config.py
  ├── src/
  │   ├── bronze_ingestion.py
  │   ├── silver_transformations.py
  │   ├── gold_sales_customers.py
  │   └── utils/
  │       ├── logger.py
  │       └── helpers.py
      run_etl.py (to run the whole pipeline)
  ├── data/
  │   ├── bronze/raw   
  │   ├── silver/silver 
  │   └── gold/      
  ├── tests/
  │   └── test_pipeline.py
  ├── README.md
  └── requirements.txt
docker-compose.yml 
```

---

## ⚙️ How It Works

### 1. Bronze Layer
- Reads the Kaggle sales CSV data.  
- Renames all columns to `snake_case`.  
- Adds metadata columns:
  - `file_path`
  - `execution_datetime`
- Stores the cleaned data as **Parquet**, partitioned by **order_date (year, month, day)**.

### 2. Silver Layer
- Applies transformations such as:
  - Type casting  
  - Date parsing  
  - Deduplication  
- Writes to the silver layer as **Parquet**.

### 3. Gold Layer
- Splits data into two curated tables:
  - **Sales**
    - `order_id`, `order_date`, `shipment_date`, `shipment_mode`, `city`
  - **Customers**
    - `customer_id`, `first_name`, `last_name`, `segment`, `country`
    - Aggregations:
      - Orders in last 1, 6, and 12 months (as of Dec 30, 2018)
      - Total lifetime orders
- Customer dataset is **fully rewritten** on each run.

### 4. Logging & Metadata
All ETL steps log progress and errors to help trace execution.  
Each table includes metadata for auditing and reproducibility.

---

## 🧪 Running the Pipeline

### Prerequisites
- Python 3.9+
- PySpark
- docker and docker compose installed

### Setup

```bash
git clone https://github.com/<your-username>/hema-etl-assessment.git
cd hema-etl-assessment
change the volume to the folder of the scripts (this syncs the code into the docker container that runs spark)
docker compose up
get the name of the container from the docker-compose file
docker exec -it nameofcontainer /bin/bash
this takes you inside the docker compose file

pip install -r requirements.txt
```

### Run

```bash
python src/bronze_ingestion.py
python src/silver_transformations.py
python src/gold_sales_customers.py
```

All outputs will be written as Parquet files under the `data/` folder.

---

## ☁️ AWS Architecture (Part 2)

This setup runs in a **multi-account environment**:

- **Account A – Data Platform**  
  Hosts the data lake and ETL jobs.  
- **Account B – Analytics**  
  Used by analysts who need to query the gold-layer data.

### How it works

1. **Data stored in S3**
   - All bronze, silver, and gold data is stored in Amazon S3 in Account A.  
   - Buckets are partitioned by order date (`year`, `month`, `day`) and written in Parquet format.

2. **Cross-account access**
   - Account A’s S3 bucket policy allows read-only access to a specific IAM role from Account B.  
   - The IAM role in Account B assumes that access and can read the shared gold-layer paths.  
   - No Lake Formation or data replication is used — just clean, policy-based access.

3. **Glue & Athena**
   - AWS Glue in Account A maintains the data catalog (schemas for the gold tables).  
   - The catalog is shared with Account B using **AWS Resource Access Manager (RAM)**.
   - Aws Glue CrawlerCrawls the folder, and gets the schema and catalogs it in aws glue catalog for ease of reading 
   - Analysts in Account B query the gold-layer data through **Athena**, which uses the shared Glue catalog and cross-account S3 access.

4. **Security**
   - Access is tightly scoped to specific S3 prefixes (only the `gold/` layer).  
   - All queries and data access are logged through **CloudTrail** and **CloudWatch**.

### High-level diagram

```
+---------------------------+        +----------------------------+
| Account A (Data Platform) |        | Account B (Analytics)      |
|---------------------------|        |----------------------------|
| S3 (bronze/silver/gold)   | <----> | Athena (query gold data)   |
| Glue Data Catalog         |  |-->  | Shared via AWS RAM         |
| IAM bucket policy         |        | IAM role w/ cross-account  |
+---------------------------+        +----------------------------+
```

This setup keeps all data in one place while letting analysts in another AWS account query the gold-layer tables securely and directly.

---

## 🧹 Notes
- Pipeline is schema-flexible: if new columns appear, they’re handled dynamically.  
- Code is modular and easy to extend for future transformations.  
- Logs and metadata make debugging and auditing straightforward.


