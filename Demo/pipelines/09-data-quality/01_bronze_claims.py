# Insurance Claims Pipeline - Bronze Layer
# Module: Implement and manage data quality constraints
#
# This pipeline ingests raw insurance claims from the source Delta table and
# applies validation checks (nullability, cardinality, range) using
# Lakeflow Spark Declarative Pipelines expectations.
#
# Industry context: InsureCo receives claim submissions from broker systems.
# Data quality issues are common - missing claim IDs, negative amounts,
# duplicate submissions, and future-dated claims all need to be caught early.
#
# How to use this file as a trainer demo:
#   1. Create a Lakeflow Spark Declarative Pipeline in your Databricks workspace
#   2. Point the pipeline source to this file (or upload to workspace)
#   3. Set the target catalog/schema to trainer_demo.demo_09
#   4. Run the pipeline in Development mode
#   5. Examine the Data quality tab for each table in the pipeline UI

from pyspark import pipelines as dp

# ─────────────────────────────────────────────
# DEMO 1A: Nullability checks with warn action
#   Show: ALL records flow through, violations are just logged
# ─────────────────────────────────────────────
@dp.table(comment="Raw claims with nullability warnings - warn action keeps all records")
@dp.expect("valid_claim_id", "claim_id IS NOT NULL")
@dp.expect("valid_policy_id", "policy_id IS NOT NULL")
@dp.expect("valid_claimant", "claimant_name IS NOT NULL")
def bronze_claims_warn():
    return spark.readStream.table("trainer_demo.demo_09.raw_claims")


# ─────────────────────────────────────────────
# DEMO 1B: Nullability checks with drop action
#   Show: Records with null claim_id or policy_id are removed
# ─────────────────────────────────────────────
@dp.table(comment="Cleaned claims - nullability enforced with drop action")
@dp.expect_or_drop("valid_claim_id", "claim_id IS NOT NULL")
@dp.expect_or_drop("valid_policy_id", "policy_id IS NOT NULL")
def bronze_claims_nullable():
    return spark.readStream.table("trainer_demo.demo_09.raw_claims")


# ─────────────────────────────────────────────
# DEMO 1C: Range checking on claim amount
#   Show: Claims with negative amounts or unreasonably high amounts are dropped
#   Business rule: Claims must be between $1 and $5,000,000
# ─────────────────────────────────────────────
@dp.table(comment="Claims with valid amount ranges enforced")
@dp.expect_or_drop("valid_claim_id", "claim_id IS NOT NULL")
@dp.expect_or_drop("valid_policy_id", "policy_id IS NOT NULL")
@dp.expect_or_drop("positive_claim_amount", "claim_amount > 0")
@dp.expect_or_drop("reasonable_claim_amount", "claim_amount <= 5000000")
@dp.expect_or_drop("claim_not_future_dated", "claim_date <= current_date()")
def bronze_claims_range_checked():
    return spark.readStream.table("trainer_demo.demo_09.raw_claims")


# ─────────────────────────────────────────────
# DEMO 1D: Cardinality check - detect duplicate claim IDs
#   Show: Flag duplicate submissions using a materialized view
#   Note: Streaming tables cannot use window functions directly;
#         use a materialized view on static data for cardinality reporting
# ─────────────────────────────────────────────
@dp.table(
    comment="Duplicate claim ID report - cardinality validation",
    table_properties={"quality": "bronze"}
)
def bronze_claims_duplicates():
    from pyspark.sql.functions import count, col
    df = spark.table("trainer_demo.demo_09.raw_claims")
    return (
        df.groupBy("claim_id")
          .agg(count("*").alias("submission_count"))
          .filter(col("submission_count") > 1)
    )


# ─────────────────────────────────────────────
# DEMO 1E: Fail action for critical violations
#   Show: Pipeline stops if a record has a null adjuster_id on an approved claim
#   (Approved claims MUST have an assigned adjuster - data integrity critical)
# ─────────────────────────────────────────────
@dp.table(comment="Approved claims - adjuster assignment is critical (fail on violation)")
@dp.expect_or_fail(
    "approved_claims_have_adjuster",
    "status != 'Approved' OR adjuster_id IS NOT NULL"
)
def bronze_approved_claims():
    return (
        spark.readStream
             .table("trainer_demo.demo_09.raw_claims")
             .filter("status = 'Approved'")
    )
