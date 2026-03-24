# Hospitality Pipeline — Bronze Layer (Streaming Ingestion)
# Module: Design and implement data pipelines
#
# Industry context:
#   StayWell Hotels operates 50+ properties across Europe and the Middle East.
#   Every night, reservation events are exported by each property-management
#   system (PMS) and dropped as JSON files into cloud storage.  This pipeline
#   ingests those raw files into the Bronze layer using Auto Loader so that
#   each record is processed exactly once.
#
# Demo objectives:
#   - Show that the INGESTION stage in the medallion architecture maps to
#     streaming tables that read append-only sources.
#   - Show the difference between a streaming table (@dp.table) and a
#     materialised view — here we just capture raw data with no business logic.
#   - Emphasise "preserve data in its original form" as the Bronze principle.
#
# How to use this file as a trainer demo:
#   1. Create a Lakeflow Spark Declarative Pipeline in your Databricks workspace.
#   2. Set the target catalog / schema to trainer_demo.demo_10.
#   3. Add this file (and the other 0x_*.py files) as pipeline source files.
#   4. Run the pipeline in Development mode.
#   5. Walk through the DAG — point out this is the entry-point / ingest node.

from pyspark import pipelines as dp


# ─────────────────────────────────────────────
# STREAMING TABLE: bronze_reservations
#   Reads from the Delta source table that mimics a raw PMS export landing zone.
#   In a real deployment the source would be read_files() against a Volume path.
#
#   Trainer points:
#     - STREAM keyword / readStream = incremental, exactly-once processing.
#     - No transformations at this layer — we store what we received.
#     - The table is append-only; historical data is never modified here.
# ─────────────────────────────────────────────
@dp.table(
    comment=(
        "Raw reservation events ingested from the property-management system. "
        "Bronze layer: no transformations, data preserved in original form."
    )
)
def bronze_reservations():
    return (
        spark.readStream
             .table("trainer_demo.demo_10.raw_reservations")
    )


# ─────────────────────────────────────────────
# STREAMING TABLE: bronze_guests
#   Ingests raw guest profile records.
#   Some records intentionally have missing or invalid fields (see setup_10)
#   so downstream expectations have something to catch.
# ─────────────────────────────────────────────
@dp.table(
    comment=(
        "Raw guest profile records from the loyalty CRM system. "
        "Bronze layer: raw state preserved for auditing and reprocessing."
    )
)
def bronze_guests():
    return (
        spark.readStream
             .table("trainer_demo.demo_10.raw_guests")
    )
