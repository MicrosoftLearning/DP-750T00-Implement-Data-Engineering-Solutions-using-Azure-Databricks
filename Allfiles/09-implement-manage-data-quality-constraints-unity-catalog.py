# ==============================================================
# ClearCover Insurance — Claims Data Quality Pipeline
# Lakeflow Spark Declarative Pipelines
# ==============================================================
#
# Complete the exercises below to build a pipeline that enforces
# data quality constraints at every layer.
# ==============================================================

from pyspark import pipelines as dp
from pyspark.sql.functions import col, count, sum as spark_sum


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

@dp.table(name='silver.claims_validated')
# TODO Exercise 3: Add @dp.expect_or_drop and other expectation decorators here as described in the instructions above.
def claims_validated():
    '''Silver: validated insurance claims with quality constraints applied.'''

    # TODO Exercise 4: Add withColumn calls here using col().cast() before the return.
    # See the Exercise 4 instructions below before modifying this function.

    return spark.readStream.table('insurance_lab.bronze.claims_raw')


# --------------------------------------------------------------
# Exercise 5: Schema Drift — Rescued Data
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

@dp.table(name='silver.claims_rescued')
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

@dp.table(name='gold.claims_summary')
@dp.table(name='gold.claims_summary')
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