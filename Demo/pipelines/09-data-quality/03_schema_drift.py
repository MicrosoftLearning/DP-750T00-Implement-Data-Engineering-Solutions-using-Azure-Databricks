# Insurance Policy Pipeline - Schema Drift Handling
# Module: Implement and manage data quality constraints
#
# This pipeline demonstrates how to detect and respond to schema drift in
# insurance policy data feeds. Broker systems evolve their data structures
# over time - adding new fields (e.g. risk_score, broker_code) or renaming
# columns - which can silently break pipelines if unhandled.
#
# How to use this file as a trainer demo:
#   1. Add this file to your Lakeflow Spark Declarative Pipeline
#   2. Run the initial pipeline with raw_policies (original schema)
#   3. Simulate schema drift: add new columns to raw_policies (see SQL in notebook)
#   4. Re-run the pipeline and observe how each strategy handles the change
#
# Schema drift strategies demonstrated:
#   A. Fail on new columns (strictest - use for critical master data)
#   B. Rescue unexpected data (capture new columns without changing schema)
#   C. Allow schema evolution (auto-add new columns to target table)

from pyspark import pipelines as dp
from pyspark.sql.functions import current_timestamp, lit, to_json, struct


# ─────────────────────────────────────────────
# DEMO 3A: Strict mode - fail if schema changes
#   Show: When broker adds unexpected columns, pipeline fails immediately
#   This forces the engineering team to review and approve structural changes
#
#   Trainer note: this is achieved via Auto Loader for file-based ingestion.
#   For Delta table sources, schema enforcement is built into Delta Lake.
#   Demonstrate by trying to insert a row with an extra column:
#
#     ALTER TABLE trainer_demo.demo_09.raw_policies
#     ADD COLUMN risk_score DOUBLE;
#
#     INSERT INTO trainer_demo.demo_09.raw_policies
#     VALUES ('POL-NEW-001', 1, 'Auto', '2025-01-01', '2026-01-01', 1200.0, 50000.0, 0.42);
#
#   The streaming pipeline reading raw_policies will detect the schema change
#   on restart and raise an AnalysisException (schema evolution not enabled).
# ─────────────────────────────────────────────
@dp.table(comment="Policies - strict schema, no evolution (demonstrates fail on drift)")
@dp.expect_or_drop("valid_policy_id", "policy_id IS NOT NULL")
@dp.expect_or_drop("valid_premium", "premium_amount > 0")
@dp.expect_or_drop("valid_coverage", "coverage_amount > 0")
@dp.expect_or_drop("valid_dates", "start_date < end_date")
def bronze_policies_strict():
    return spark.readStream.table("trainer_demo.demo_09.raw_policies")


# ─────────────────────────────────────────────
# DEMO 3B: Rescue mode - capture unexpected columns in a JSON column
#   Show: Original schema is preserved; new/unexpected columns go to _rescued_data
#   This lets the pipeline keep running without interruption.
#   After the run, query _rescued_data to explore what arrived:
#
#     SELECT policy_id, _rescued_data
#     FROM trainer_demo.demo_09.bronze_policies_rescued
#     WHERE _rescued_data IS NOT NULL
#     LIMIT 20;
# ─────────────────────────────────────────────
@dp.table(comment="Policies with rescued data - unexpected columns stored in _rescued_data")
def bronze_policies_rescued():
    # When using Auto Loader (file-based), set schemaEvolutionMode=rescue
    # For Delta table sources, unexpected columns are handled by select
    raw = spark.readStream.table("trainer_demo.demo_09.raw_policies")
    known_cols = ["policy_id", "customer_id", "policy_type",
                  "start_date", "end_date", "premium_amount", "coverage_amount"]
    existing = [c for c in raw.columns if c in known_cols]
    extra = [c for c in raw.columns if c not in known_cols]
    if extra:
        rescued = to_json(struct(*[raw[c] for c in extra]))
        return raw.select(*existing).withColumn("_rescued_data", rescued)
    return raw.select(*existing).withColumn("_rescued_data", lit(None).cast("string"))


# ─────────────────────────────────────────────
# DEMO 3C: Error handling - quarantine records missing required structure
#   Show: Policies with null required fields are routed to quarantine
#   The main pipeline continues processing valid records
# ─────────────────────────────────────────────
@dp.table(comment="Valid policies passing structural checks")
def bronze_policies_valid():
    return (
        spark.readStream
             .table("trainer_demo.demo_09.raw_policies")
             .filter(
                 "policy_id IS NOT NULL "
                 "AND customer_id IS NOT NULL "
                 "AND start_date IS NOT NULL "
                 "AND end_date IS NOT NULL"
             )
    )


@dp.table(comment="Quarantined policies missing required structural fields")
def bronze_policies_quarantine():
    return (
        spark.readStream
             .table("trainer_demo.demo_09.raw_policies")
             .filter(
                 "policy_id IS NULL "
                 "OR customer_id IS NULL "
                 "OR start_date IS NULL "
                 "OR end_date IS NULL"
             )
             .withColumn("quarantined_at", current_timestamp())
             .withColumn("reason", lit("Missing required structural fields"))
    )
