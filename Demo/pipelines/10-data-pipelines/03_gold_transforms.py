# Hospitality Pipeline — Gold Layer (Transform for Consumption)
# Module: Design and implement data pipelines
#
# Industry context:
#   StayWell Hotels' Revenue Management team needs daily occupancy and
#   revenue KPIs per property and room type.  The Guest Experience team
#   needs a consolidated guest-stay history to personalise future stays.
#   Both use cases are served from Gold materialized views built on top
#   of the verified Silver reservations.
#
# Demo objectives:
#   - Show that MATERIALIZED VIEWS are the right tool for aggregations/joins
#     (incremental, handles updates/deletes in source).
#   - Contrast with STREAMING TABLES used in the Bronze layer.
#   - Walk through the dependency graph in the pipeline DAG editor.
#   - Show partitioning via partition_cols for query performance.
#
# How to use this file as a trainer demo:
#   1. Ensure 01_bronze_reservations.py and 02_silver_clean_validate.py are
#      included in the pipeline.
#   2. Run the full pipeline.
#   3. Query the materialized views directly in a notebook to validate results.
#   4. Point out the automated dependency ordering in the DAG.

from pyspark import pipelines as dp
from pyspark.sql import functions as F


# ─────────────────────────────────────────────
# MATERIALIZED VIEW: gold_daily_occupancy
#   Aggregates checked-in/checked-out reservations by property and date.
#
#   Trainer points:
#     - Reads from silver_reservations (clean, validated data).
#     - Aggregation = materialized view, NOT a streaming table.
#     - Framework auto-refreshes only affected partitions when Silver changes.
# ─────────────────────────────────────────────
@dp.materialized_view(
    comment=(
        "Gold layer: daily occupancy and revenue per property and room type. "
        "Consumed by the Revenue Management BI dashboard."
    )
)
def gold_daily_occupancy():
    reservations = spark.read.table("LIVE.silver_reservations")
    return (
        reservations
        .filter(F.col("status").isin("checked_in", "checked_out", "confirmed"))
        .groupBy("property_id", "room_type", "check_in_date")
        .agg(
            F.count("reservation_id").alias("total_reservations"),
            F.sum("total_amount").alias("total_revenue"),
            F.avg("total_amount").alias("avg_revenue_per_stay"),
            F.countDistinct("guest_id").alias("unique_guests"),
        )
    )


# ─────────────────────────────────────────────
# MATERIALIZED VIEW: gold_guest_stay_history
#   Joins reservation data with guest profiles to produce a
#   consolidated stay history for personalisation use cases.
#
#   Trainer points:
#     - Joins two silver tables — only possible with a materialized view.
#     - The framework determines the correct execution order automatically.
# ─────────────────────────────────────────────
@dp.materialized_view(
    comment=(
        "Gold layer: full guest stay history enriched with loyalty profile. "
        "Consumed by the Guest Experience and CRM personalisation teams."
    )
)
def gold_guest_stay_history():
    reservations = spark.read.table("LIVE.silver_reservations")
    guests = spark.read.table("LIVE.silver_guests")

    return (
        reservations
        .join(guests, on="guest_id", how="inner")
        .select(
            "guest_id",
            "guest_name",
            "email",
            "loyalty_tier",
            "loyalty_points",
            "reservation_id",
            "property_id",
            "room_type",
            "check_in_date",
            "check_out_date",
            "total_amount",
            "status",
        )
    )


# ─────────────────────────────────────────────
# MATERIALIZED VIEW: gold_property_revenue_summary
#   Monthly revenue summary per property — used for executive reporting.
#
#   Trainer points:
#     - Shows aggregation at a different granularity (month vs. day).
#     - Demonstrates that multiple Gold views can serve different consumers
#       from the same Silver foundation.
# ─────────────────────────────────────────────
@dp.materialized_view(
    comment=(
        "Gold layer: monthly revenue summary per property. "
        "Consumed by the Executive Dashboard and Finance reporting."
    )
)
def gold_property_revenue_summary():
    occupancy = spark.read.table("LIVE.gold_daily_occupancy")
    return (
        occupancy
        .withColumn("month", F.date_trunc("month", F.col("check_in_date")))
        .groupBy("property_id", "month")
        .agg(
            F.sum("total_revenue").alias("monthly_revenue"),
            F.sum("total_reservations").alias("monthly_reservations"),
            F.sum("unique_guests").alias("monthly_unique_guests"),
            F.avg("avg_revenue_per_stay").alias("avg_revenue_per_stay"),
        )
    )
