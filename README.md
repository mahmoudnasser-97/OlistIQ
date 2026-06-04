# OlistIQ — End-to-End Data Engineering Platform

A complete data engineering platform built on the real-world **Olist Brazilian E-Commerce dataset** (Kaggle). Implements both a **batch processing pipeline** and a **real-time streaming pipeline** inside a fully containerized Docker environment.

---

## Architecture

### Streaming Pipeline
```
simulator.py ──► Kafka ──► Spark Structured Streaming ──► Redis ──► Streamlit
(Faker events)  (broker)                                 (counters)  Dashboard
```

### Batch Pipeline
```
CSV Files ──► Bronze ──► Silver ──► Gold ──► PostgreSQL
(9 sources)  (MinIO)    (MinIO)    (MinIO)  (2 schemas)
                  └────── Airflow DAG ───────┘
```

---

## Tech Stack

| Layer | Technology |
|---|---|
| Ingestion | Apache Kafka (KRaft), Faker simulator |
| Stream Processing | Apache Spark Structured Streaming |
| Batch Processing | Apache Spark 3.5.1 |
| Object Storage | MinIO (Bronze / Silver / Gold buckets) |
| Data Warehouse | PostgreSQL 15 |
| Orchestration | Apache Airflow 2.7.1 |
| Serving Layer | Redis 7.2 |
| Dashboard | Streamlit + Plotly |
| Infrastructure | Docker Compose (16 containers) |

---

## Project Structure

```
OlistIQ/
├── docker-compose.yml              ← Full 16-container infrastructure
├── simulator.py                    ← Run on host machine to generate live events
├── sql/
│   └── init.sql                    ← PostgreSQL schema (auto-runs on first boot)
├── dags/
│   └── olist_pipeline_dag.py       ← Airflow DAG (4 sequential tasks)
├── shared/
│   └── scripts/
│       ├── bronze/
│       │   └── bronze_ingestion.py
│       ├── silver/
│       │   └── silver_transformation.py
│       └── gold/
│           ├── gold_delivery_performance.py
│           └── gold_delivery_performance.py
├── spark/
│   └── Dockerfile                  ← Custom Spark image (Kafka + S3A + PostgreSQL JARs)
├── spark_jobs/
│   └── streaming_kafka_to_redis.py ← Spark Structured Streaming job
├── streamlit/
│   ├── Dockerfile
│   └── dashboard.py                ← Live operations dashboard
├── kafka-connect/
│   └── Dockerfile
└── jupyter/
    └── Dockerfile
```

---

## Batch Pipeline — Medallion Architecture

Three layers stored in MinIO as Parquet files:

| Layer | Bucket | Description |
|---|---|---|
| Bronze | `s3a://bronze/olist/` | Raw CSV files — no transformations |
| Silver | `s3a://silver/olist/` | Cleaned, standardized, validated |
| Gold | `s3a://gold/olist/` | Dimensional model — star schema |

### Two Datamarts in PostgreSQL

**delivery_performance schema** — 6 tables:
- `dim_date`, `dim_customer`, `dim_seller`, `dim_product`
- `fct_order_delivery` — one row per order with delivery metrics
- `fct_seller_fulfillment` — one row per order item with freight metrics

**customer_churn schema** — 5 tables:
- `dim_date`, `dim_product`, `dim_customer_profile`
- `fct_customer_orders` — order sequence and gap analysis
- `fct_churn_summary` — one row per unique customer behavioral fingerprint

### Airflow DAG

```
bronze_ingestion → silver_transformation → gold_delivery_performance → gold_customer_churn
```

Tasks run sequentially — prevents single Spark worker overload.

---

## Streaming Pipeline

```
simulator.py → Kafka (olist_orders_stream) → Spark Structured Streaming → Redis → Streamlit
```

- **simulator.py** — runs on host machine, generates synthetic Olist order events every 2 seconds using Faker with Brazilian locale
- **Kafka** — KRaft mode (no Zookeeper), topic: `olist_orders_stream`
- **Spark Streaming** — foreachBatch sink, 10-second micro-batches, flattens nested JSON and writes pre-aggregated counters to Redis
- **Redis** — stores running KPI counters (total orders, revenue, status breakdowns, payment types, state distribution)
- **Streamlit** — auto-refreshes every 5 seconds showing 5 KPI cards, order status bar chart, payment donut, top categories, Brazilian state choropleth map, and live event feed

---

## How to Run

### Prerequisites
- Docker Desktop with WSL2 backend
- Python 3.9+ on host machine (for simulator)
- Git Bash or any terminal

### 1. Clone the repo
```bash
git clone https://github.com/mahmoudnasser-97/OlistIQ.git
cd OlistIQ
```

### 2. Add source data
Place the 9 Olist CSV files from Kaggle into `shared/data/`:
```
olist_customers_dataset.csv
olist_geolocation_dataset.csv
olist_order_items_dataset.csv
olist_order_payments_dataset.csv
olist_order_reviews_dataset.csv
olist_orders_dataset.csv
olist_products_dataset.csv
olist_sellers_dataset.csv
product_category_name_translation.csv
```

### 3. Start all containers
```bash
docker-compose up -d
```
Wait 2-3 minutes for all services to initialize.

### 4. Run the batch pipeline
Open Airflow at **http://localhost:8089** (admin / admin)
→ Find DAG: `olist_batch_pipeline`
→ Click Trigger

### 5. Run the streaming pipeline
Install simulator dependencies on host machine:
```bash
pip install kafka-python faker
```
Then run:
```bash
python simulator.py
```

### 6. View dashboards

| Service | URL | Credentials |
|---|---|---|
| Streamlit Dashboard | http://localhost:8501 | — |
| Airflow | http://localhost:8089 | admin / admin |
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin |
| Spark UI | http://localhost:8080 | — |
| Kafka UI | http://localhost:8090 | — |
| pgAdmin | http://localhost:8085 | admin@admin.com / admin |
| Jupyter | http://localhost:8888 | Token: olist |

---

## Dataset

[Olist Brazilian E-Commerce Dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) — Kaggle

9 CSV files covering orders, customers, sellers, products, payments, reviews, and geolocation data from Brazil's largest e-commerce marketplace.

---

## Key Technical Decisions

- **Sequential Gold tasks in Airflow** — parallel execution overloads a single Spark worker causing TaskResultLost errors
- **MinIO over HDFS** — S3-compatible API, simpler setup, works identically with Spark S3A connector
- **KRaft Kafka** — no Zookeeper required, fewer containers, simpler configuration
- **Pre-aggregation in Redis** — Spark writes running totals, Streamlit reads instantly regardless of data volume
- **foreachBatch** — most flexible Spark Streaming sink, allows arbitrary Redis writes per micro-batch
- **Pure Spark SQL** — no Python UDFs anywhere, avoids Python version mismatch between Airflow and Spark workers
