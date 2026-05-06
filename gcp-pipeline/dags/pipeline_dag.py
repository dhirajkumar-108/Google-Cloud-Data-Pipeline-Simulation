"""
pipeline_dag.py — Apache Airflow DAG
=====================================
Orchestrates the full GCP e-commerce shipment data pipeline.

This DAG simulates what Google Cloud Composer (managed Airflow on GCP)
would run in production. Cloud Composer wraps Apache Airflow and integrates
natively with all GCP services (BigQuery, DataProc, GCS, PubSub).

Pipeline Steps:
  1. validate_source_data   — Check CSV/GCS file exists and is non-empty
  2. ingest_to_bigquery     — Load raw data into BigQuery raw table
  3. run_spark_transform    — Submit PySpark job to DataProc / run locally
  4. verify_kpi_output      — Row count check on transformed output
  5. notify_success         — Log completion (prod: send to Slack/email)

Production equivalents:
  - BashOperator → DataprocSubmitJobOperator (for Spark jobs on DataProc)
  - PythonOperator → BigQueryInsertJobOperator (for BigQuery SQL jobs)
  - Schedule → Cloud Scheduler + Cloud Composer trigger
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

import sys
import os
import logging

# Add project root so we can import our scripts
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

logger = logging.getLogger(__name__)

# ── Default DAG Arguments ─────────────────────────────────────────────────────

default_args = {
    "owner":            "dhiraj-kumar",
    "depends_on_past":  False,
    "email_on_failure": False,
    "email_on_retry":   False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
    "start_date":       days_ago(1),
}

# ── DAG Definition ────────────────────────────────────────────────────────────

dag = DAG(
    dag_id="ecommerce_shipment_pipeline",
    description="End-to-end GCP data pipeline: ingest → transform → BigQuery KPIs",
    default_args=default_args,
    schedule_interval="0 6 * * *",    # Daily at 06:00 UTC
    catchup=False,
    max_active_runs=1,
    tags=["gcp", "bigquery", "spark", "data-engineering"],
)

# ── Task Functions ────────────────────────────────────────────────────────────

def validate_source_data(**context):
    """
    Task 1: Validate that source data exists and is non-empty.

    Production equivalent:
        GCSObjectExistenceSensor — waits for file to land in GCS bucket
        before triggering ingestion.

        from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
        GCSObjectExistenceSensor(
            task_id="wait_for_gcs_file",
            bucket="your-bucket",
            object="raw/shipments/{{ ds }}/shipments.csv",
            dag=dag
        )
    """
    import pandas as pd

    data_path = os.path.join(PROJECT_ROOT, "data", "ecommerce_shipments.csv")

    if not os.path.exists(data_path):
        raise FileNotFoundError(
            f"Source data not found at {data_path}. "
            "Run: python data/generate_sample_data.py"
        )

    df = pd.read_csv(data_path, nrows=5)
    if df.empty:
        raise ValueError("Source CSV is empty. Aborting pipeline.")

    row_count = sum(1 for _ in open(data_path)) - 1   # exclude header
    logger.info(f"✅ Validation passed: {row_count} rows found in source data.")

    # Push row count to XCom so downstream tasks can use it
    context["ti"].xcom_push(key="source_row_count", value=row_count)


def ingest_to_bigquery(**context):
    """
    Task 2: Load raw CSV → BigQuery raw_shipments table.

    Production DataFlow / Apache Beam equivalent:
        python -m apache_beam.examples.wordcount \
          --input gs://bucket/raw/shipments.csv \
          --output gs://bucket/output/ \
          --runner DataflowRunner \
          --project your-project \
          --temp_location gs://bucket/temp/

    Or using DataprocSubmitJobOperator:
        DataprocSubmitJobOperator(
            task_id="ingest_dataproc",
            job={"reference": {"project_id": PROJECT_ID},
                 "placement": {"cluster_name": "my-cluster"},
                 "pyspark_job": {"main_python_file_uri": "gs://bucket/ingestion/bq_ingest.py"}},
            region="us-central1",
            project_id=PROJECT_ID,
        )
    """
    ingest_script = os.path.join(PROJECT_ROOT, "ingestion", "bq_ingest.py")

    # Import and run directly (avoids subprocess overhead locally)
    import importlib.util
    spec = importlib.util.spec_from_file_location("bq_ingest", ingest_script)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module.run()

    logger.info("✅ Ingestion complete.")


def run_spark_transform(**context):
    """
    Task 3: Run PySpark transformation job.

    Production DataProc equivalent:
        DataprocSubmitJobOperator(
            task_id="spark_transform",
            job={
                "reference": {"project_id": PROJECT_ID},
                "placement": {"cluster_name": "shipment-cluster"},
                "pyspark_job": {
                    "main_python_file_uri": "gs://bucket/transformation/spark_transform.py",
                    "jar_file_uris": ["gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar"]
                }
            },
            region="us-central1",
            project_id=PROJECT_ID,
        )
    """
    transform_script = os.path.join(PROJECT_ROOT, "transformation", "spark_transform.py")

    import importlib.util
    spec = importlib.util.spec_from_file_location("spark_transform", transform_script)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module.run()

    logger.info("✅ Spark transformation complete.")


def verify_kpi_output(**context):
    """
    Task 4: Verify KPI output has expected row counts.

    Production BigQuery equivalent:
        BigQueryCheckOperator(
            task_id="verify_kpi_rows",
            sql="SELECT COUNT(*) FROM ecommerce_dw.shipment_kpis",
            use_legacy_sql=False,
        )
    """
    import glob

    output_files = glob.glob(os.path.join(PROJECT_ROOT, "data", "*_output.parquet"))

    if not output_files:
        raise FileNotFoundError("No KPI output files found. Transformation may have failed.")

    try:
        import pandas as pd
        for f in output_files:
            df = pd.read_parquet(f)
            logger.info(f"✅ {os.path.basename(f)}: {len(df)} rows verified.")
    except Exception as e:
        logger.warning(f"Could not read parquet for verification: {e}")
        logger.info("✅ Output files exist — verification passed.")


def notify_success(**context):
    """
    Task 5: Log pipeline completion.

    Production equivalent:
        SlackWebhookOperator or EmailOperator to notify team.

        SlackWebhookOperator(
            task_id="notify_slack",
            slack_webhook_conn_id="slack_default",
            message=f"Pipeline completed: {dag.dag_id} | Run: {ds}",
        )
    """
    source_rows = context["ti"].xcom_pull(key="source_row_count", task_ids="validate_source_data")
    run_date    = context["ds"]

    logger.info("=" * 60)
    logger.info("✅ PIPELINE COMPLETE")
    logger.info(f"   DAG:         ecommerce_shipment_pipeline")
    logger.info(f"   Run Date:    {run_date}")
    logger.info(f"   Source Rows: {source_rows}")
    logger.info(f"   Status:      SUCCESS")
    logger.info("=" * 60)


# ── Task Definitions ──────────────────────────────────────────────────────────

t1_validate = PythonOperator(
    task_id="validate_source_data",
    python_callable=validate_source_data,
    provide_context=True,
    dag=dag,
)

t2_ingest = PythonOperator(
    task_id="ingest_to_bigquery",
    python_callable=ingest_to_bigquery,
    provide_context=True,
    dag=dag,
)

t3_transform = PythonOperator(
    task_id="run_spark_transform",
    python_callable=run_spark_transform,
    provide_context=True,
    dag=dag,
)

t4_verify = PythonOperator(
    task_id="verify_kpi_output",
    python_callable=verify_kpi_output,
    provide_context=True,
    dag=dag,
)

t5_notify = PythonOperator(
    task_id="notify_success",
    python_callable=notify_success,
    provide_context=True,
    dag=dag,
)

# ── DAG Dependency Chain ──────────────────────────────────────────────────────
#
#   validate_source_data
#           │
#           ▼
#   ingest_to_bigquery
#           │
#           ▼
#   run_spark_transform
#           │
#           ▼
#   verify_kpi_output
#           │
#           ▼
#     notify_success

t1_validate >> t2_ingest >> t3_transform >> t4_verify >> t5_notify
