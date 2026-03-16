# ==============================================================
# ClearCover Insurance — Claims Data Quality Pipeline
# Lakeflow Spark Declarative Pipelines
# ==============================================================
#
# You are a data engineer at ClearCover Insurance.
# Raw claims data arrives daily from regional offices and partner
# brokers. The data is inconsistent: required fields are sometimes
# absent, amounts use string formats or carry negative values,
# dates are occasionally malformed, and the source schema may
# silently gain new columns over time.
#
# Complete the exercises below to build a pipeline that enforces
# data quality constraints at every layer.
#
# Data flow:
#   insurance_lab.bronze.claims_raw    — raw landing table (provided)
#   insurance_lab.silver.claims_validated — Exercises 3 + 4
#   insurance_lab.silver.claims_rescued   — Exercise 5
#   insurance_lab.gold.claims_summary     — provided (no changes needed)
# ==============================================================

import databricks.sdk.pipelines as dp
from pyspark.sql.functions import try_cast, col, count, sum as spark_sum


# --------------------------------------------------------------
# Exercise 3: Nullability and Status Validation
#
# Add expectations to the claims_validated() function below to:
#
#   3a) DROP records where claim_id IS NULL
#   3b) DROP records where customer_id IS NULL
#   3c) WARN (keep) records where status is NOT IN
#       ('OPEN', 'PENDING', 'CLOSED')
#   3d) FAIL the pipeline if any record has coverage_amount <= 0
#
# Use the decorators:
#   @dp.expect_or_drop(name, condition)   — drops violating rows
#   @dp.expect(name, condition)           — warns, keeps all rows
#   @dp.expect_or_fail(name, condition)   — fails the pipeline
#
# 🤖 Ask the Databricks Assistant:
#   "Show me how to add expect_or_drop, expect, and expect_or_fail
#    decorators to a Lakeflow Spark Declarative Pipelines Python
#    function to enforce nullability and status constraints"
# --------------------------------------------------------------

@dp.table(schema_name='silver', name='claims_validated')
# TODO 3a: @dp.expect_or_drop — 'valid_claim_id'    — claim_id IS NOT NULL
# TODO 3b: @dp.expect_or_drop — 'valid_customer_id' — customer_id IS NOT NULL
# TODO 3c: @dp.expect        — 'valid_status'       — status IN ('OPEN', 'PENDING', 'CLOSED')
# TODO 3d: @dp.expect_or_fail — 'valid_coverage'    — coverage_amount > 0
def claims_validated():
    '''Silver: validated insurance claims with quality constraints applied.'''

    # TODO Exercise 4: Add withColumn calls here using try_cast before the return.
    # See the Exercise 4 instructions below before modifying this function.

    return spark.readStream.table('insurance_lab.bronze.claims_raw')


# --------------------------------------------------------------
# Exercise 4: Data Type Checks
#
# Update claims_validated() above to cast string columns to their
# correct types and drop records where casting fails:
#
#   4a) Use try_cast to convert claim_date  → DATE
#       Add @dp.expect_or_drop 'valid_claim_date'   — claim_date IS NOT NULL
#       (rows where try_cast returned NULL had an unparseable date)
#
#   4b) Use try_cast to convert claim_amount → DECIMAL(12,2)
#       Add @dp.expect_or_drop 'valid_claim_amount' — claim_amount IS NOT NULL
#       (rows where try_cast returned NULL had an unparseable amount)
#
#   4c) Add @dp.expect_or_drop 'non_negative_amount' — claim_amount >= 0
#
# 💡 Hint: call .withColumn('claim_date', try_cast(col('claim_date'), 'date'))
#          inside the function body and return the transformed dataframe.
#
# 🤖 Ask the Databricks Assistant:
#   "In PySpark, use withColumn and try_cast to convert a streaming
#    dataframe column from STRING to DATE, and another column from
#    STRING to DECIMAL(12,2). Then show me the matching pipeline
#    expectations that drop rows where casting produced NULL."
# --------------------------------------------------------------


# --------------------------------------------------------------
# Exercise 5: Schema Drift — Rescued Data
#
# ClearCover's source system occasionally adds new columns to the
# claims file without prior notice. Complete claims_rescued() to
# handle this gracefully using Auto Loader with rescue mode.
#
# Configure the readStream to:
#   - Read from /Volumes/insurance_lab/bronze/raw_files/
#   - Use cloudFiles format with cloudFiles.format = csv
#   - Set cloudFiles.schemaLocation to a path inside the volume:
#       /Volumes/insurance_lab/bronze/raw_files/_schema
#   - Set cloudFiles.schemaEvolutionMode to 'rescue'
#   - Set rescuedDataColumn to '_rescued_data'
#   - Set cloudFiles.inferColumnTypes to 'true'
#   - Set header to 'true'
#
# When the source file matches the expected schema, _rescued_data
# will be NULL. Any unexpected new columns are captured as JSON
# in that column instead of crashing the pipeline.
#
# 🤖 Ask the Databricks Assistant:
#   "Write a PySpark Auto Loader readStream using cloudFiles format
#    csv with schemaEvolutionMode rescue and a _rescued_data column
#    to capture unexpected new columns from schema drift"
# --------------------------------------------------------------

@dp.table(schema_name='silver', name='claims_rescued')
def claims_rescued():
    '''Silver: raw claims loaded via Auto Loader with rescue schema evolution.'''
    # TODO Exercise 5: Replace the pass statement below with an Auto Loader
    # readStream implementation as described in the instructions above.
    pass


# --------------------------------------------------------------
# Gold: Claims Summary — provided, no changes needed
#
# This table aggregates validated silver claims by claim type and
# status, producing a summary for reporting dashboards.
# --------------------------------------------------------------

@dp.table(schema_name='gold', name='claims_summary')
def claims_summary():
    '''Gold: aggregate claim counts and total amounts per type and status.'''
    return (
        spark.read.table('insurance_lab.silver.claims_validated')
        .groupBy('claim_type', 'status')
        .agg(
            count('claim_id').alias('claim_count'),
            spark_sum('claim_amount').alias('total_claim_amount')
        )
    )
