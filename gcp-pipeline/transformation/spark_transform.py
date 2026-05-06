"""
spark_transform.py — PySpark Batch Transformation
==================================================
Simulates the transformation layer of a GCP DataProc pipeline.

In production this would run on:
  - Google DataProc (managed Spark/Hadoop cluster)
  - Triggered by Cloud Composer (Airflow) DAG
  - Read from BigQuery raw table, write back to BigQuery KPI table

Locally it reads the raw CSV (or simulated parquet), applies
PySpark transformations, and writes KPI output to parquet.

GCP Services Demonstrated:
  - DataProc (Spark engine on GCP)
  - BigQuery (read/write via spark-bigquery-connector)
  - DataFlow / Apache Beam equivalent patterns documented in comments
"""

import os
import sys
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.config import (
    GCP_PROJECT_ID, BQ_DATASET, BQ_RAW_TABLE, BQ_KPI_TABLE,
    SPARK_APP_NAME, SPARK_MASTER, LOCAL_DATA_PATH
)

# ── Conditional PySpark import ────────────────────────────────────────────────
try:
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window
    from pyspark.sql.types import (
        StructType, StructField, StringType, IntegerType, FloatType, DateType
    )
    SPARK_AVAILABLE = True
except ImportError:
    SPARK_AVAILABLE = False
    logger.warning("PySpark not installed. Falling back to Pandas simulation.")
    import pandas as pd


# ── Spark Session Factory ─────────────────────────────────────────────────────

def create_spark_session():
    """
    Creates a local SparkSession.

    On DataProc (production), this would be:
        SparkSession.builder
            .appName(SPARK_APP_NAME)
            .config("spark.jars", "gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar")
            .getOrCreate()
    The BigQuery connector jar lets Spark read/write directly from BigQuery.
    """
    spark = (
        SparkSession.builder
        .appName(SPARK_APP_NAME)
        .master(SPARK_MASTER)
        .config("spark.sql.shuffle.partitions", "4")   # Low for local; use 200+ on cluster
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    logger.info(f"SparkSession created: {SPARK_APP_NAME} | master={SPARK_MASTER}")
    return spark


# ── Data Loading ──────────────────────────────────────────────────────────────

def load_data(spark):
    """
    Load raw data into Spark DataFrame.

    Production equivalent (DataProc + BigQuery connector):
        df = spark.read.format("bigquery") \
            .option("table", f"{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_RAW_TABLE}") \
            .load()

    Apache Beam / DataFlow equivalent (streaming):
        with beam.Pipeline() as p:
            rows = (p | beam.io.ReadFromBigQuery(
                        query=f"SELECT * FROM {BQ_DATASET}.{BQ_RAW_TABLE}",
                        use_standard_sql=True))
    """
    # Try simulated parquet first (output of bq_ingest local mode)
    parquet_path = "data/raw_shipments_simulated.parquet"
    if os.path.exists(parquet_path):
        logger.info(f"Loading from simulated parquet: {parquet_path}")
        return spark.read.parquet(parquet_path)

    logger.info(f"Loading from CSV: {LOCAL_DATA_PATH}")
    return spark.read.option("header", "true").option("inferSchema", "true").csv(LOCAL_DATA_PATH)


# ── Transformations ───────────────────────────────────────────────────────────

def clean_data(df):
    """
    Step 1: Data Cleaning
    - Drop nulls on key columns
    - Cast types
    - Add derived columns
    """
    logger.info("Step 1: Cleaning data...")

    df = df.dropna(subset=["shipment_id", "warehouse_block"])

    # Cast to correct types
    df = (df
        .withColumn("customer_care_calls", F.col("customer_care_calls").cast(IntegerType()))
        .withColumn("customer_rating",     F.col("customer_rating").cast(IntegerType()))
        .withColumn("cost_of_product",     F.col("cost_of_product").cast(FloatType()))
        .withColumn("weight_in_gms",       F.col("weight_in_gms").cast(IntegerType()))
        .withColumn("scheduled_days",      F.col("scheduled_days").cast(IntegerType()))
        .withColumn("actual_days",         F.col("actual_days").cast(IntegerType()))
        .withColumn("on_time_delivery",    F.col("on_time_delivery").cast(IntegerType()))
    )

    # Derived columns
    df = (df
        .withColumn("delay_days",
            F.when(F.col("actual_days") > F.col("scheduled_days"),
                   F.col("actual_days") - F.col("scheduled_days"))
            .otherwise(F.lit(0)))
        .withColumn("weight_kg",
            F.round(F.col("weight_in_gms") / 1000, 2))
        .withColumn("is_high_value",
            F.when(F.col("cost_of_product") > 2500, F.lit(1)).otherwise(F.lit(0)))
    )

    logger.info(f"Clean dataset: {df.count()} rows, {len(df.columns)} columns")
    return df


def compute_warehouse_kpis(df):
    """
    Step 2: Warehouse-Level KPIs
    - On-time delivery rate
    - Average delay
    - Total shipments
    - Average customer rating

    Window functions used — equivalent to BigQuery analytic functions.
    """
    logger.info("Step 2: Computing warehouse KPIs...")

    warehouse_kpis = df.groupBy("warehouse_block").agg(
        F.count("shipment_id").alias("total_shipments"),
        F.round(F.avg("on_time_delivery") * 100, 2).alias("on_time_delivery_pct"),
        F.round(F.avg("delay_days"), 2).alias("avg_delay_days"),
        F.round(F.avg("customer_rating"), 2).alias("avg_customer_rating"),
        F.round(F.avg("cost_of_product"), 2).alias("avg_product_cost"),
        F.sum("is_high_value").alias("high_value_shipments"),
    )

    # Window function: rank warehouses by on-time delivery (like BigQuery RANK())
    window = Window.orderBy(F.desc("on_time_delivery_pct"))
    warehouse_kpis = warehouse_kpis.withColumn("warehouse_rank", F.rank().over(window))

    return warehouse_kpis


def compute_route_kpis(df):
    """
    Step 3: Route-Level Risk Analysis
    Identifies high-risk routes with delay rate above 30%.
    """
    logger.info("Step 3: Computing route KPIs...")

    route_kpis = df.groupBy("route").agg(
        F.count("shipment_id").alias("total_shipments"),
        F.round(
            (1 - F.avg("on_time_delivery")) * 100, 2
        ).alias("delay_rate_pct"),
        F.round(F.avg("delay_days"), 2).alias("avg_delay_days"),
    ).withColumn(
        "risk_level",
        F.when(F.col("delay_rate_pct") > 40, F.lit("HIGH"))
        .when(F.col("delay_rate_pct") > 20, F.lit("MEDIUM"))
        .otherwise(F.lit("LOW"))
    )

    return route_kpis


def compute_category_kpis(df):
    """
    Step 4: Product Category Analysis
    Monthly aggregation by product category — simulates a time-partitioned
    BigQuery table (partitioned on shipment_date).
    """
    logger.info("Step 4: Computing category KPIs...")

    category_kpis = df.groupBy("product_category", "mode_of_shipment").agg(
        F.count("shipment_id").alias("shipment_count"),
        F.round(F.avg("on_time_delivery") * 100, 2).alias("on_time_pct"),
        F.round(F.avg("customer_rating"), 2).alias("avg_rating"),
        F.round(F.avg("discount_offered"), 2).alias("avg_discount"),
        F.round(F.sum("cost_of_product"), 2).alias("total_revenue"),
    )

    return category_kpis


# ── Write Output ──────────────────────────────────────────────────────────────

def write_output(df, name: str):
    """
    Write transformed DataFrame to output.

    Production (DataProc → BigQuery):
        df.write.format("bigquery") \
            .option("table", f"{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_KPI_TABLE}") \
            .option("temporaryGcsBucket", GCS_BUCKET) \
            .mode("overwrite") \
            .save()

    Apache Beam / DataFlow equivalent:
        rows | beam.io.WriteToBigQuery(
                    table=f"{project}:{dataset}.{table}",
                    schema=SCHEMA,
                    write_disposition=WriteToBigQuery.WRITE_TRUNCATE)
    """
    output_path = f"data/{name}_output.parquet"
    df.write.mode("overwrite").parquet(output_path)
    logger.info(f"[OUTPUT] Saved {name} → {output_path} ({df.count()} rows)")

    # Show sample in console
    print(f"\n── {name.upper()} SAMPLE ───────────────────────────────────")
    df.show(5, truncate=False)


# ── Pandas Fallback (no PySpark) ─────────────────────────────────────────────

def run_pandas_simulation():
    """Runs the same logic using Pandas when PySpark is not installed."""
    logger.info("[PANDAS MODE] PySpark not found. Running Pandas simulation...")

    df = pd.read_csv(LOCAL_DATA_PATH)
    df["delay_days"] = (df["actual_days"] - df["scheduled_days"]).clip(lower=0)
    df["is_high_value"] = (df["cost_of_product"] > 2500).astype(int)

    warehouse_kpis = df.groupby("warehouse_block").agg(
        total_shipments=("shipment_id", "count"),
        on_time_delivery_pct=("on_time_delivery", lambda x: round(x.mean() * 100, 2)),
        avg_delay_days=("delay_days", lambda x: round(x.mean(), 2)),
        avg_customer_rating=("customer_rating", lambda x: round(x.mean(), 2)),
        avg_product_cost=("cost_of_product", lambda x: round(x.mean(), 2)),
    ).reset_index()

    route_kpis = df.groupby("route").agg(
        total_shipments=("shipment_id", "count"),
        delay_rate_pct=("on_time_delivery", lambda x: round((1 - x.mean()) * 100, 2)),
        avg_delay_days=("delay_days", lambda x: round(x.mean(), 2)),
    ).reset_index()

    warehouse_kpis.to_parquet("data/warehouse_kpis_output.parquet", index=False)
    route_kpis.to_parquet("data/route_kpis_output.parquet", index=False)

    print("\n── WAREHOUSE KPIs ────────────────────────────────────────")
    print(warehouse_kpis.to_string(index=False))
    print("\n── ROUTE KPIs ────────────────────────────────────────────")
    print(route_kpis.to_string(index=False))
    logger.info("[PANDAS MODE] Outputs saved to data/ folder.")


# ── Main ──────────────────────────────────────────────────────────────────────

def run():
    if not SPARK_AVAILABLE:
        run_pandas_simulation()
        return

    spark = create_spark_session()

    try:
        raw_df   = load_data(spark)
        clean_df = clean_data(raw_df)

        warehouse_kpis = compute_warehouse_kpis(clean_df)
        route_kpis     = compute_route_kpis(clean_df)
        category_kpis  = compute_category_kpis(clean_df)

        write_output(warehouse_kpis, "warehouse_kpis")
        write_output(route_kpis,     "route_kpis")
        write_output(category_kpis,  "category_kpis")

        logger.info("✅ Transformation complete.")

    finally:
        spark.stop()


if __name__ == "__main__":
    run()
