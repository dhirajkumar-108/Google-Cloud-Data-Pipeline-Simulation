# 🚀 GCP Data Pipeline — E-Commerce Shipment Analytics

An end-to-end data engineering project built on **Google Cloud Platform**, simulating a production-grade pipeline for e-commerce shipment data. Covers batch ingestion, PySpark transformation, BigQuery data warehousing, and Apache Airflow orchestration.

---

## 🏗️ Architecture

```
Raw CSV Data (Local / GCS)
        │
        ▼
[Python Ingestion Script]
  google-cloud-bigquery
        │
        ▼
[BigQuery Raw Table]
  dataset: ecommerce_dw
  table:   raw_shipments
        │
        ▼
[PySpark Transformation]
  Cleaning → Aggregation → Partitioning
        │
        ▼
[BigQuery Analytics Table]
  table: shipment_kpis
        │
        ▼
[Apache Airflow DAG]
  Orchestrates all steps on schedule
        │
        ▼
[BigQuery Dashboard-Ready Layer]
  Optimized for BI tools (Looker, Tableau, Power BI)
```

---

## 🛠️ Tech Stack

| Layer | Tools |
|---|---|
| Cloud Platform | Google Cloud Platform (GCP) |
| Data Warehouse | BigQuery |
| Batch Processing | PySpark (Apache Spark) |
| Orchestration | Apache Airflow (Cloud Composer equivalent) |
| Streaming Concept | PubSub → DataFlow pattern (documented) |
| Language | Python 3.9+ |
| Storage | Google Cloud Storage (GCS) / Local CSV |
| Version Control | GitHub |

---

## 📁 Project Structure

```
gcp-data-pipeline/
│
├── data/
│   └── ecommerce_shipments.csv        # Sample dataset
│
├── ingestion/
│   └── bq_ingest.py                   # Load CSV → BigQuery raw table
│
├── transformation/
│   └── spark_transform.py             # PySpark cleaning & KPI aggregation
│
├── dags/
│   └── pipeline_dag.py                # Airflow DAG orchestration
│
├── sql/
│   └── create_tables.sql              # BigQuery table schemas
│
├── config/
│   └── config.py                      # Project config
│
└── requirements.txt
```

---

## ⚙️ Setup & Run

### 1. Prerequisites
- Python 3.9+
- Google Cloud SDK (`gcloud` CLI)
- A GCP project with BigQuery API enabled
- Apache Airflow (`pip install apache-airflow`)

### 2. Install dependencies
```bash
pip install -r requirements.txt
```

### 3. Authenticate with GCP
```bash
gcloud auth application-default login
```

### 4. Update config
Edit `config/config.py` with your GCP Project ID and dataset name.

### 5. Run ingestion
```bash
python ingestion/bq_ingest.py
```

### 6. Run PySpark transformation
```bash
spark-submit transformation/spark_transform.py
```

### 7. Run Airflow DAG
```bash
airflow db init
cp dags/pipeline_dag.py ~/airflow/dags/
airflow scheduler &
airflow webserver
```
Open `http://localhost:8080` and trigger `ecommerce_shipment_pipeline`

---

## 📊 KPIs Tracked

- **On-Time Delivery Rate** — % of shipments delivered within SLA
- **Average Shipment Delay** — Mean delay in days per warehouse/route
- **Customer Rating Distribution** — Ratings across product categories
- **Warehouse Performance Score** — Volume vs. delay per warehouse block
- **High-Risk Routes** — Routes with delay rate above threshold

---

## ☁️ GCP Services Used / Simulated

| Service | Usage |
|---|---|
| **BigQuery** | Data Warehouse — raw + analytics tables |
| **Cloud Storage (GCS)** | Raw file staging layer |
| **DataProc / PySpark** | Batch transformation engine |
| **Cloud Composer / Airflow** | Pipeline orchestration & scheduling |
| **PubSub + DataFlow** | Streaming ingestion pattern (documented in code) |

---

## 👤 Author

**Dhiraj Kumar** — [LinkedIn](https://linkedin.com/in/dhiraj-kumar108) | [GitHub](https://github.com/dhirajkumar-108)
