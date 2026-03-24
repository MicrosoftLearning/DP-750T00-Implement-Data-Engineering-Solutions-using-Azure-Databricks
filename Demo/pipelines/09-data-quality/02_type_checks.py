# Insurance Claims Pipeline - Data Type Checks
# Module: Implement and manage data quality constraints
#
# This pipeline demonstrates data type validation for insurance claims and
# policy data ingested from broker systems. Raw data arrives as strings and
# needs to be validated and cast to appropriate types before silver-layer storage.
#
# How to use this file as a trainer demo:
#   1. Add this file to your Lakeflow Spark Declarative Pipeline
#   2. Run the pipeline in Development mode
#   3. Discuss the difference between cast (fail hard) vs try_cast (return NULL)
#   4. Show the quarantine table capturing records that fail type validation

from pyspark import pipelines as dp
from pyspark.sql.functions import col, current_timestamp, lit


# ─────────────────────────────────────────────
# DEMO 2A: Schema enforcement demonstration
#   Show: Delta Lake enforces column types on write automatically.
#   The raw_policies table stores amounts as DOUBLE; broker feeds may send strings.
#   Trainer: run a manual INSERT with a string amount to show enforcement:
#     INSERT INTO trainer_demo.demo_09.raw_policies
#     VALUES ('POL-BAD-001', 999, 'Auto', '2024-01-01', '2025-01-01', 'fifteenhundred', 50000.0)
#   → This will FAIL because 'fifteenhundred' can't be cast to DOUBLE
# ─────────────────────────────────────────────

# ─────────────────────────────────────────────
# DEMO 2B: Pipeline expectations for type checking using try_cast
#   Show: Records where claim_amount or claim_date can't be cast are dropped
# ─────────────────────────────────────────────
@dp.table(comment="Type-validated claims - invalid amounts and dates are dropped")
@dp.expect_or_drop("valid_claim_id", "claim_id IS NOT NULL")
@dp.expect_or_drop("valid_amount_type", "claim_amount > 0")
@dp.expect_or_drop("valid_claim_type", "claim_type IN ('Auto', 'Property', 'Life', 'Health', 'Liability')")
@dp.expect_or_drop("valid_status", "status IN ('Submitted', 'Under Review', 'Approved', 'Denied')")
def bronze_type_validated_claims():
    return spark.readStream.table("trainer_demo.demo_09.raw_claims")


# ─────────────────────────────────────────────
# DEMO 2C: Quarantine pattern - separate valid from invalid records
#   Show: Valid records go to silver, invalid records to quarantine table
#   Trainer: run both tables, compare counts
# ─────────────────────────────────────────────
@dp.table(comment="Valid claims ready for silver processing")
def silver_valid_claims():
    return (
        spark.readStream
             .table("trainer_demo.demo_09.raw_claims")
             .filter(
                 "claim_id IS NOT NULL "
                 "AND policy_id IS NOT NULL "
                 "AND claim_amount > 0 "
                 "AND claim_amount <= 5000000 "
                 "AND claim_date <= current_date() "
                 "AND claim_type IN ('Auto', 'Property', 'Life', 'Health', 'Liability') "
                 "AND status IN ('Submitted', 'Under Review', 'Approved', 'Denied')"
             )
    )


@dp.table(comment="Quarantined claims that failed type or value validation")
def bronze_quarantine_claims():
    return (
        spark.readStream
             .table("trainer_demo.demo_09.raw_claims")
             .filter(
                 "claim_id IS NULL "
                 "OR policy_id IS NULL "
                 "OR claim_amount <= 0 "
                 "OR claim_amount > 5000000 "
                 "OR claim_date > current_date() "
                 "OR claim_type NOT IN ('Auto', 'Property', 'Life', 'Health', 'Liability') "
                 "OR status NOT IN ('Submitted', 'Under Review', 'Approved', 'Denied')"
             )
             .withColumn("quarantined_at", current_timestamp())
             .withColumn("quarantine_reason", lit("Type or value validation failed"))
    )
