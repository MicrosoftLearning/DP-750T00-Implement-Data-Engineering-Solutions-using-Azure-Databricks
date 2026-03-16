# ==============================================================
# ClearCover Insurance — Claims Data Quality Pipeline
# INSTRUCTOR ANSWER KEY
# ==============================================================

from pyspark import pipelines as dp
from pyspark.sql.functions import try_cast, col, count, sum as spark_sum


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
        .table('insurance_lab.bronze.claims_raw')
        .withColumn('claim_date',   try_cast(col('claim_date'),   'date'))
        .withColumn('claim_amount', try_cast(col('claim_amount'), 'decimal(12,2)'))
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
        .option('cloudFiles.schemaLocation',      '/Volumes/insurance_lab/bronze/raw_files/_schema')
        .option('cloudFiles.schemaEvolutionMode', 'rescue')
        .option('rescuedDataColumn',              '_rescued_data')
        .option('cloudFiles.inferColumnTypes',    'true')
        .load('/Volumes/insurance_lab/bronze/raw_files/')
    )


# --------------------------------------------------------------
# Gold: Claims Summary
# --------------------------------------------------------------

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
