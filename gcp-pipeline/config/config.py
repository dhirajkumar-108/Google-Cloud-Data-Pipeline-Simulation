# =============================================================
# config.py — GCP Data Pipeline Configuration
# Update these values before running the pipeline
# =============================================================

# ── GCP Settings ──────────────────────────────────────────────
GCP_PROJECT_ID  = "your-gcp-project-id"       # Replace with your GCP Project ID
GCP_REGION      = "us-central1"               # Change region if needed

# ── BigQuery Settings ──────────────────────────────────────────
BQ_DATASET      = "ecommerce_dw"              # BigQuery dataset name
BQ_RAW_TABLE    = "raw_shipments"             # Raw ingestion table
BQ_KPI_TABLE    = "shipment_kpis"             # Transformed KPI output table
BQ_STREAM_TABLE = "realtime_events"           # Simulated streaming events table

# ── Google Cloud Storage ───────────────────────────────────────
GCS_BUCKET      = "your-gcs-bucket-name"      # Replace with your GCS bucket
GCS_RAW_PREFIX  = "raw/shipments/"

# ── Local Data ─────────────────────────────────────────────────
LOCAL_DATA_PATH = "data/ecommerce_shipments.csv"

# ── Airflow / Schedule ─────────────────────────────────────────
PIPELINE_SCHEDULE   = "0 6 * * *"             # Daily at 06:00 UTC
PIPELINE_START_DATE = "2024-01-01"

# ── Spark ──────────────────────────────────────────────────────
SPARK_APP_NAME  = "GCP_Shipment_Transform"
SPARK_MASTER    = "local[*]"                  # Use "yarn" on a real DataProc cluster
