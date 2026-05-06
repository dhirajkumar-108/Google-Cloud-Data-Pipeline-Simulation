"""
bq_ingest.py — Batch Ingestion: CSV → BigQuery Raw Table
=========================================================
Simulates the ingestion layer of a GCP data pipeline.

In production this would:
  - Read from Google Cloud Storage (GCS)
  - Use DataFlow / DataProc for large-scale ingestion
  - Trigger via Cloud Scheduler or Cloud Composer (Airflow)

Locally it reads from data/ecommerce_shipments.csv and loads
into BigQuery using the Python client library.

GCP Services Demonstrated:
  - google-cloud-bigquery  (Data Warehouse ingestion)
  - google-cloud-storage   (Staging layer concept)
  - PubSub streaming pattern documented in comments below
"""

import os
import sys
import logging
from datetime import datetime

import pandas as pd

# ── Conditional GCP import (falls back gracefully if not installed) ──
try:
    from google.cloud import bigquery
    from google.cloud.exceptions import NotFound
    GCP_AVAILABLE = True
except ImportError:
    GCP_AVAILABLE = False
    logging.warning("google-cloud-bigquery not installed. Running in LOCAL SIMULATION mode.")

# Add project root to path for config import
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.config import (
    GCP_PROJECT_ID, BQ_DATASET, BQ_RAW_TABLE, LOCAL_DATA_PATH
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)


# ── BigQuery Schema Definition ───────────────────────────────────────────────
# Defines the raw_shipments table schema.
# In BigQuery, partitioning on shipment_date enables cost-efficient queries.

RAW_TABLE_SCHEMA = [
    bigquery.SchemaField("shipment_id",        "STRING",  mode="REQUIRED") if GCP_AVAILABLE else None,
    bigquery.SchemaField("customer_id",        "STRING",  mode="NULLABLE") if GCP_AVAILABLE else None,
    bigquery.SchemaField("product_category",   "STRING",  mode="NULLABLE") if GCP_AVAILABLE else None,
    bigquery.SchemaField("warehouse_block",    "STRING",  mode="NULLABLE") if GCP_AVAILABLE else None,
    bigquery.SchemaField("mode_of_shipment",   "STRING",  mode="NULLABLE") if GCP_AVAILABLE else None,
    bigquery.SchemaField("route",              "STRING",  mode="NULLABLE") if GCP_AVAILABLE else None,
    bigquery.SchemaField("customer_care_calls","INTEGER", mode="NULLABLE") if GCP_AVAILABLE else None,
    bigquery.SchemaField("customer_rating",    "INTEGER", mode="NULLABLE") if GCP_AVAILABLE else None,
    bigquery.SchemaField("cost_of_product",    "FLOAT",   mode="NULLABLE") if GCP_AVAILABLE else None,
    bigquery.SchemaField("prior_purchases",    "INTEGER", mode="NULLABLE") if GCP_AVAILABLE else None,
    bigquery.SchemaField("discount_offered",   "FLOAT",   mode="NULLABLE") if GCP_AVAILABLE else None,
    bigquery.SchemaField("weight_in_gms",      "INTEGER", mode="NULLABLE") if GCP_AVAILABLE else None,
    bigquery.SchemaField("scheduled_days",     "INTEGER", mode="NULLABLE") if GCP_AVAILABLE else None,
    bigquery.SchemaField("actual_days",        "INTEGER", mode="NULLABLE") if GCP_AVAILABLE else None,
    bigquery.SchemaField("on_time_delivery",   "INTEGER", mode="NULLABLE") if GCP_AVAILABLE else None,
    bigquery.SchemaField("shipment_date",      "DATE",    mode="NULLABLE") if GCP_AVAILABLE else None,
    bigquery.SchemaField("ingested_at",        "TIMESTAMP", mode="NULLABLE") if GCP_AVAILABLE else None,
] if GCP_AVAILABLE else []


def load_csv(path: str) -> pd.DataFrame:
    """Load and perform basic validation on raw CSV."""
    logger.info(f"Reading data from: {path}")
    df = pd.read_csv(path)

    # Add ingestion timestamp (pipeline metadata)
    df["ingested_at"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    # Basic validation
    initial_count = len(df)
    df = df.dropna(subset=["shipment_id"])
    df["shipment_date"] = pd.to_datetime(df["shipment_date"], errors="coerce").dt.date.astype(str)

    logger.info(f"Loaded {initial_count} rows | {len(df)} valid rows after null check")
    return df


def create_dataset_if_not_exists(client, dataset_id: str):
    """Create BigQuery dataset if it doesn't already exist."""
    dataset_ref = f"{GCP_PROJECT_ID}.{dataset_id}"
    try:
        client.get_dataset(dataset_ref)
        logger.info(f"Dataset {dataset_ref} already exists.")
    except NotFound:
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "US"
        client.create_dataset(dataset)
        logger.info(f"Created dataset: {dataset_ref}")


def load_to_bigquery(df: pd.DataFrame):
    """
    Load DataFrame into BigQuery raw table.

    Uses WRITE_TRUNCATE for daily full refresh (idempotent).
    In production, use WRITE_APPEND + deduplication for incremental loads.

    DataFlow / Apache Beam equivalent:
        beam.io.WriteToBigQuery(table, schema=schema,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE)
    """
    client = bigquery.Client(project=GCP_PROJECT_ID)
    create_dataset_if_not_exists(client, BQ_DATASET)

    table_id = f"{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_RAW_TABLE}"

    job_config = bigquery.LoadJobConfig(
        schema=RAW_TABLE_SCHEMA,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        # Partition on shipment_date for cost-efficient querying
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="shipment_date"
        ),
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=0,
        autodetect=False,
    )

    logger.info(f"Loading {len(df)} rows into {table_id} ...")
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()  # Wait for job to complete

    table = client.get_table(table_id)
    logger.info(f"✅ Loaded {table.num_rows} rows into {table_id}")


def simulate_local(df: pd.DataFrame):
    """
    Local simulation mode — runs when GCP credentials are not available.
    Saves the processed DataFrame to a local parquet file to simulate
    what would be written to BigQuery.

    PubSub Streaming Pattern (production equivalent):
        publisher = pubsub_v1.PublisherClient()
        topic_path = publisher.topic_path(project_id, topic_id)
        for _, row in df.iterrows():
            data = json.dumps(row.to_dict()).encode("utf-8")
            publisher.publish(topic_path, data=data)
        # DataFlow would subscribe and write to BigQuery in real-time
    """
    output_path = "data/raw_shipments_simulated.parquet"
    df.to_parquet(output_path, index=False)
    logger.info(f"[LOCAL MODE] Saved {len(df)} rows to {output_path}")
    logger.info("[LOCAL MODE] In production this loads directly into BigQuery.")
    logger.info("[LOCAL MODE] Streaming equivalent: PubSub → DataFlow → BigQuery")

    # Print sample
    print("\n── Sample Output (first 5 rows) ──────────────────────────")
    print(df.head().to_string(index=False))
    print("──────────────────────────────────────────────────────────\n")


def run():
    df = load_csv(LOCAL_DATA_PATH)

    if GCP_AVAILABLE:
        logger.info("GCP credentials found. Loading to BigQuery...")
        load_to_bigquery(df)
    else:
        logger.info("Running in LOCAL SIMULATION mode.")
        simulate_local(df)


if __name__ == "__main__":
    run()
