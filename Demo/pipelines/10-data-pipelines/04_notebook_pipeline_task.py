# Hospitality Pipeline — Notebook-based Pipeline (Procedural Approach)
# Module: Design and implement data pipelines
# Unit:   Create pipeline with notebook / task dependencies
#
# Industry context:
#   StayWell Hotels needs a nightly report that calculates cancellation rates
#   per property. Because the logic requires a custom Python function to
#   classify cancellation reasons (which is hard to express declaratively),
#   this processing step is implemented as a notebook task inside a Lakeflow Job.
#
# Demo objectives:
#   - Show the PROCEDURAL / notebook approach: read → transform → write.
#   - Demonstrate dbutils.widgets for runtime parameters.
#   - Show dbutils.notebook.exit() to signal success/failure to the job.
#   - Show pass_parameters_between_tasks using dbutils.jobs.taskValues.
#   - This notebook is intended to be a TASK in a Lakeflow Job, NOT run
#     standalone. The trainer should create a Job with two notebook tasks:
#       Task 1: this file  (ingest_cancellations)
#       Task 2: a second notebook that reads the output table
#     and configure Task 2 to depend on Task 1.
#
# NOTE: This file is a plain Python script for trainer reference.
#       Copy the code blocks into separate workspace notebooks when demoing.

# ── CELL 1: Widget / parameter setup ─────────────────────────────────────────
# Trainer: show how parameters make notebooks flexible and reusable across runs.

dbutils.widgets.text("process_date",  "", "Process Date (YYYY-MM-DD)")
dbutils.widgets.text("property_id",   "ALL", "Property ID (or ALL)")

process_date = dbutils.widgets.get("process_date") or str(spark.sql("SELECT current_date()").collect()[0][0])
property_id  = dbutils.widgets.get("property_id")

print(f"Processing cancellations for: {process_date}, property: {property_id}")


# ── CELL 2: Read from Silver reservations ────────────────────────────────────
# Trainer: in a notebook pipeline we explicitly read → transform → write.

from pyspark.sql import functions as F

reservations = spark.read.table("trainer_demo.demo_10.silver_reservations")

# Filter to cancelled reservations on the process date
cancelled = reservations.filter(
    (F.col("status") == "cancelled") &
    (F.col("check_in_date") == process_date)
)

if property_id != "ALL":
    cancelled = cancelled.filter(F.col("property_id") == property_id)

print(f"Found {cancelled.count()} cancelled reservations")


# ── CELL 3: Custom business logic (reason for cancellation) ──────────────────
# Trainer: this logic is the reason we chose a notebook over Lakeflow Pipelines.
# Complex Python branching is hard to express declaratively.

from pyspark.sql.types import StringType

def classify_cancellation_reason(days_before_checkin):
    """Classify cancellation reason based on lead time."""
    if days_before_checkin is None:
        return "unknown"
    elif days_before_checkin <= 1:
        return "last_minute"
    elif days_before_checkin <= 7:
        return "short_term"
    elif days_before_checkin <= 30:
        return "medium_term"
    else:
        return "long_term"

classify_udf = F.udf(classify_cancellation_reason, StringType())

from datetime import date

enriched = (
    cancelled
    .withColumn(
        "days_before_checkin",
        F.datediff(F.col("check_in_date"), F.lit(date.today()))
    )
    .withColumn("cancellation_reason", classify_udf(F.col("days_before_checkin")))
)


# ── CELL 4: Write output and signal job task result ───────────────────────────
# Trainer: show dbutils.notebook.exit() — downstream tasks can read this value.

try:
    (
        enriched
        .select("reservation_id", "guest_id", "property_id",
                "check_in_date", "total_amount",
                "cancellation_reason", "days_before_checkin")
        .write
        .format("delta")
        .mode("append")
        .partitionBy("check_in_date")
        .saveAsTable("trainer_demo.demo_10.cancellation_analysis")
    )
    records = enriched.count()
    # Pass record count to downstream tasks via task values
    dbutils.jobs.taskValues.set(key="cancellations_processed", value=records)
    dbutils.notebook.exit(f"SUCCESS: Processed {records} cancellation records for {process_date}")

except Exception as e:
    dbutils.notebook.exit(f"FAILED: {str(e)}")
