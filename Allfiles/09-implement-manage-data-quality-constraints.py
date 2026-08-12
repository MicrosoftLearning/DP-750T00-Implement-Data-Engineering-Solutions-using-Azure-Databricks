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
# 🤖 Ask Genie Code:
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
# 🤖 Ask Genie Code:
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



# ==============================================================
# ClearCover Insurance — Claims Data Quality Pipeline
# INSTRUCTOR ANSWER KEY
# ==============================================================

from pyspark import pipelines as dp
from pyspark.sql.functions import expr, col, count, sum as spark_sum


# --------------------------------------------------------------
# Exercise 3 + 4: Nullability, Status, and Data Type Validation
# --------------------------------------------------------------

@dp.table(name='silver.claims_validated')
@dp.expect_or_drop('valid_claim_id',      'claim_id IS NOT NULL')
@dp.expect_or_drop('valid_customer_id',   'customer_id IS NOT NULL')
@dp.expect(        'valid_status',        "status IN ('OPEN', 'PENDING', 'CLOSED')")
@dp.expect_or_fail('valid_coverage',      'coverage_amount > 0')
@dp.expect_or_drop('valid_claim_date',    'claim_date IS NOT NULL')
@dp.expect_or_drop('valid_claim_amount',  'claim_amount IS NOT NULL')
@dp.expect_or_drop('non_negative_amount', 'claim_amount >= 0')
def claims_validated():
    '''Silver: validated insurance claims with full quality constraints applied.'''
    return (
        spark.readStream
        .table('insurance_lab3.bronze.claims_raw')
        .withColumn('claim_date',   expr('try_cast(claim_date AS date)'))
        .withColumn('claim_amount', expr('try_cast(claim_amount AS decimal(12,2))'))
    )


# --------------------------------------------------------------
# Exercise 5: Schema Drift — Rescued Data
# --------------------------------------------------------------

@dp.table(name='silver.claims_rescued')
def claims_rescued():
    '''Silver: raw claims loaded via Auto Loader with rescue schema evolution.'''
    return (
        spark.readStream
        .format('cloudFiles')
        .option('cloudFiles.format',              'csv')
        .option('header',                          'true')
        .option('cloudFiles.schemaLocation',      '/Volumes/insurance_lab3/bronze/raw_files/_schema')
        .option('cloudFiles.schemaEvolutionMode', 'rescue')
        .option('rescuedDataColumn',              '_rescued_data')
        .option('cloudFiles.inferColumnTypes',    'true')
        .load('/Volumes/insurance_lab3/bronze/raw_files/')
    )


# --------------------------------------------------------------
# Gold: Claims Summary
# --------------------------------------------------------------

@dp.table(name='gold.claims_summary')
def claims_summary():
    '''Gold: aggregate claim counts and total amounts per type and status.'''
    return (
        spark.read.table('insurance_lab3.silver.claims_validated')
        .groupBy('claim_type', 'status')
        .agg(
            count('claim_id').alias('claim_count'),
            spark_sum('claim_amount').alias('total_claim_amount')
        )
    )

