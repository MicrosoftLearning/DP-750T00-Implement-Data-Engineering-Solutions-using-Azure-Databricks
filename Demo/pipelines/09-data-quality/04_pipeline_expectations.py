# Insurance Claims Pipeline - Comprehensive Expectations
# Module: Implement and manage data quality constraints
#
# This pipeline demonstrates the full power of Lakeflow Spark Declarative
# Pipelines expectations: naming conventions, choosing actions, combining
# multiple expectations, and monitoring results.
#
# Scenario: InsureCo's silver-layer claims pipeline applies all data quality
# rules in a single pass. The pipeline processes auto, property, life, health
# and liability claims with business-specific validation rules per claim type.
#
# How to use this file as a trainer demo:
#   1. Add this file to your Lakeflow Spark Declarative Pipeline
#   2. Run the pipeline
#   3. Open Jobs & Pipelines → select the pipeline → click a dataset
#   4. Open the "Data quality" tab to see expectation pass/fail counts
#   5. Point out how well-named expectations make monitoring intuitive

from pyspark import pipelines as dp


# ─────────────────────────────────────────────
# Reusable expectation dictionaries
#   Trainer: highlight how expect_all_or_drop reuses rules across tables
# ─────────────────────────────────────────────
CLAIM_INTEGRITY_RULES = {
    "claim_id_not_null":  "claim_id IS NOT NULL",
    "policy_id_not_null": "policy_id IS NOT NULL",
    "adjuster_assigned":  "adjuster_id IS NOT NULL",
}

CLAIM_AMOUNT_RULES = {
    "positive_amount":       "claim_amount > 0",
    "max_claim_amount":      "claim_amount <= 5000000",
    "not_future_dated":      "claim_date <= current_date()",
    "date_after_year_2000":  "claim_date >= '2000-01-01'",
}


# ─────────────────────────────────────────────
# DEMO 4A: Three expectations components - name, constraint, action
#   Show: Each expectation has a descriptive name, a SQL constraint, and an action
#   Trainer: point to the names in the pipeline UI Data quality tab
# ─────────────────────────────────────────────
@dp.table(comment="Silver claims - all integrity and amount rules applied")
@dp.expect_all_or_drop(CLAIM_INTEGRITY_RULES)
@dp.expect_all_or_drop(CLAIM_AMOUNT_RULES)
@dp.expect_or_drop(
    "valid_claim_type",
    "claim_type IN ('Auto', 'Property', 'Life', 'Health', 'Liability')"
)
@dp.expect_or_drop(
    "valid_status",
    "status IN ('Submitted', 'Under Review', 'Approved', 'Denied')"
)
def silver_claims():
    return spark.readStream.table("trainer_demo.demo_09.raw_claims")


# ─────────────────────────────────────────────
# DEMO 4B: Warn action for monitoring quality trends
#   Show: Approved claims without an adjuster are logged but NOT blocked
#   Use this to build quality dashboards - data still flows downstream
# ─────────────────────────────────────────────
@dp.table(comment="All submitted claims with quality warnings logged (no drops)")
@dp.expect("claim_id_present",    "claim_id IS NOT NULL")
@dp.expect("policy_id_present",   "policy_id IS NOT NULL")
@dp.expect("amount_positive",     "claim_amount > 0")
@dp.expect("amount_in_range",     "claim_amount <= 5000000")
@dp.expect("status_valid",        "status IN ('Submitted', 'Under Review', 'Approved', 'Denied')")
@dp.expect("claim_type_valid",    "claim_type IN ('Auto', 'Property', 'Life', 'Health', 'Liability')")
def silver_claims_monitored():
    return spark.readStream.table("trainer_demo.demo_09.raw_claims")


# ─────────────────────────────────────────────
# DEMO 4C: Fail action for critical data integrity
#   Show: Approved claims are critical - any missing adjuster stops the pipeline
#   Trainer: explain when fail is appropriate vs drop vs warn
# ─────────────────────────────────────────────
@dp.table(comment="Approved claims - pipeline fails if adjuster is missing (critical integrity)")
@dp.expect_or_fail("claim_id_not_null", "claim_id IS NOT NULL")
@dp.expect_or_fail("policy_id_not_null", "policy_id IS NOT NULL")
@dp.expect_or_fail("adjuster_required_for_approved", 
    "status != 'Approved' OR adjuster_id IS NOT NULL")
def silver_approved_claims():
    return (
        spark.readStream
             .table("trainer_demo.demo_09.raw_claims")
             .filter("status = 'Approved'")
    )


# ─────────────────────────────────────────────
# DEMO 4D: Per-claim-type business rules combined in one expectation
#   Show: Complex business rule combining multiple conditions
#   Rule: Auto claims > $100,000 require a senior adjuster (adjuster_id < 100)
# ─────────────────────────────────────────────
@dp.table(comment="High-value auto claims with complex business rule validation")
@dp.expect_or_drop("claim_id_not_null",  "claim_id IS NOT NULL")
@dp.expect_or_drop("policy_id_not_null", "policy_id IS NOT NULL")
@dp.expect_or_drop("adjuster_not_null",  "adjuster_id IS NOT NULL")
@dp.expect(
    "high_value_auto_requires_senior_adjuster",
    "claim_type != 'Auto' OR claim_amount <= 100000 OR adjuster_id < 100"
)
def silver_high_value_auto_claims():
    return (
        spark.readStream
             .table("trainer_demo.demo_09.raw_claims")
             .filter("claim_type = 'Auto' AND claim_amount > 50000")
    )
