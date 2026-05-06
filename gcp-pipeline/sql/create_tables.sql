-- =============================================================
-- create_tables.sql — BigQuery Table Schemas
-- Run these in BigQuery console or via bq CLI
-- =============================================================
-- Replace `your-gcp-project-id` with your actual GCP Project ID


-- ── Step 1: Create Dataset ─────────────────────────────────────
-- Run in terminal:
-- bq mk --dataset your-gcp-project-id:ecommerce_dw


-- ── Raw Shipments Table ────────────────────────────────────────
-- Partitioned by shipment_date (cost-efficient for daily queries)
-- Clustered by warehouse_block (speeds up warehouse-level filters)

CREATE TABLE IF NOT EXISTS `your-gcp-project-id.ecommerce_dw.raw_shipments`
(
    shipment_id         STRING    NOT NULL,
    customer_id         STRING,
    product_category    STRING,
    warehouse_block     STRING,
    mode_of_shipment    STRING,
    route               STRING,
    customer_care_calls INT64,
    customer_rating     INT64,
    cost_of_product     FLOAT64,
    prior_purchases     INT64,
    discount_offered    FLOAT64,
    weight_in_gms       INT64,
    scheduled_days      INT64,
    actual_days         INT64,
    on_time_delivery    INT64,
    shipment_date       DATE,
    ingested_at         TIMESTAMP
)
PARTITION BY shipment_date
CLUSTER BY warehouse_block
OPTIONS (
    description = "Raw e-commerce shipment data — ingested daily via Python pipeline",
    require_partition_filter = FALSE
);


-- ── Warehouse KPI Table ────────────────────────────────────────

CREATE TABLE IF NOT EXISTS `your-gcp-project-id.ecommerce_dw.warehouse_kpis`
(
    warehouse_block         STRING,
    total_shipments         INT64,
    on_time_delivery_pct    FLOAT64,
    avg_delay_days          FLOAT64,
    avg_customer_rating     FLOAT64,
    avg_product_cost        FLOAT64,
    high_value_shipments    INT64,
    warehouse_rank          INT64,
    computed_at             TIMESTAMP
)
OPTIONS (
    description = "Warehouse-level KPIs computed by PySpark transformation"
);


-- ── Route Risk Table ───────────────────────────────────────────

CREATE TABLE IF NOT EXISTS `your-gcp-project-id.ecommerce_dw.route_kpis`
(
    route               STRING,
    total_shipments     INT64,
    delay_rate_pct      FLOAT64,
    avg_delay_days      FLOAT64,
    risk_level          STRING,
    computed_at         TIMESTAMP
)
OPTIONS (
    description = "Route-level delay risk analysis"
);


-- =============================================================
-- ANALYTICAL QUERIES (run in BigQuery console)
-- =============================================================


-- ── 1. On-Time Delivery Rate by Warehouse ─────────────────────
SELECT
    warehouse_block,
    COUNT(shipment_id)                                              AS total_shipments,
    ROUND(AVG(on_time_delivery) * 100, 2)                          AS on_time_pct,
    ROUND(AVG(GREATEST(actual_days - scheduled_days, 0)), 2)       AS avg_delay_days,
    RANK() OVER (ORDER BY AVG(on_time_delivery) DESC)              AS warehouse_rank
FROM `your-gcp-project-id.ecommerce_dw.raw_shipments`
GROUP BY warehouse_block
ORDER BY on_time_pct DESC;


-- ── 2. High-Risk Routes (delay rate > 30%) ────────────────────
SELECT
    route,
    COUNT(shipment_id)                                  AS total_shipments,
    ROUND((1 - AVG(on_time_delivery)) * 100, 2)         AS delay_rate_pct,
    ROUND(AVG(GREATEST(actual_days - scheduled_days, 0)), 2) AS avg_delay_days,
    CASE
        WHEN (1 - AVG(on_time_delivery)) * 100 > 40 THEN 'HIGH'
        WHEN (1 - AVG(on_time_delivery)) * 100 > 20 THEN 'MEDIUM'
        ELSE 'LOW'
    END AS risk_level
FROM `your-gcp-project-id.ecommerce_dw.raw_shipments`
GROUP BY route
HAVING delay_rate_pct > 30
ORDER BY delay_rate_pct DESC;


-- ── 3. Customer Rating Distribution by Category ───────────────
SELECT
    product_category,
    customer_rating,
    COUNT(*) AS shipment_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY product_category), 2) AS pct_of_category
FROM `your-gcp-project-id.ecommerce_dw.raw_shipments`
GROUP BY product_category, customer_rating
ORDER BY product_category, customer_rating;


-- ── 4. Monthly Shipment Volume Trend ──────────────────────────
SELECT
    DATE_TRUNC(shipment_date, MONTH)    AS shipment_month,
    mode_of_shipment,
    COUNT(shipment_id)                  AS total_shipments,
    ROUND(AVG(on_time_delivery) * 100, 2) AS on_time_pct,
    ROUND(SUM(cost_of_product), 2)      AS total_revenue
FROM `your-gcp-project-id.ecommerce_dw.raw_shipments`
GROUP BY shipment_month, mode_of_shipment
ORDER BY shipment_month, mode_of_shipment;


-- ── 5. 7-Day Rolling Average Delay (Window Function) ──────────
SELECT
    shipment_date,
    COUNT(shipment_id)                      AS daily_shipments,
    ROUND(AVG(GREATEST(actual_days - scheduled_days, 0)), 2) AS avg_delay,
    ROUND(
        AVG(AVG(GREATEST(actual_days - scheduled_days, 0)))
        OVER (ORDER BY shipment_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
    , 2) AS rolling_7d_avg_delay
FROM `your-gcp-project-id.ecommerce_dw.raw_shipments`
GROUP BY shipment_date
ORDER BY shipment_date;
