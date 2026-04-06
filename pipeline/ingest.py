"""
Bronze layer: ingest raw source files into Delta Parquet tables.

Each source lands in its own Delta table, completely unmodified except for
the addition of a single `ingestion_timestamp` column recording when the
batch run started (consistent across all rows in the run).

Output tables
─────────────
  {bronze_path}/accounts/      — raw accounts.csv
  {bronze_path}/customers/     — raw customers.csv
  {bronze_path}/transactions/  — raw transactions.jsonl (nested structs preserved)

Design notes
────────────
* CSV files are read with inferSchema=false — all columns arrive as STRING,
  matching the "as-arrived" Bronze contract.  Type casting happens in Silver.
* JSONL is read without a fixed schema — Spark infers types from the JSON
  (numerics stay numeric, booleans stay boolean, nested objects become structs).
  This preserves the wire-format semantics of the source.
* The ingestion timestamp is a single Python datetime captured once before any
  Spark work begins so all three tables carry the same run-start timestamp.
* Delta is written in overwrite mode — Bronze is a full-reload layer; there is
  no incremental merge at this stage.
"""

import logging
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType

from pipeline.spark_utils import get_or_create_spark, load_config

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Public entry point
# ─────────────────────────────────────────────────────────────────────────────

def run_ingestion(config: dict = None) -> None:
    """Ingest all three source files into the Bronze Delta layer."""
    if config is None:
        config = load_config()

    spark = get_or_create_spark(config)

    # One timestamp for the entire run — all Bronze rows carry the same value.
    ingestion_ts = datetime.now(timezone.utc)
    logger.info("Bronze ingestion run started at %s", ingestion_ts.isoformat())

    input_cfg  = config["input"]
    bronze_root = config["output"]["bronze_path"]

    _ingest_csv(
        spark,
        src=input_cfg["accounts_path"],
        dst=f"{bronze_root}/accounts",
        ingestion_ts=ingestion_ts,
        label="accounts",
    )
    _ingest_csv(
        spark,
        src=input_cfg["customers_path"],
        dst=f"{bronze_root}/customers",
        ingestion_ts=ingestion_ts,
        label="customers",
    )
    _ingest_jsonl(
        spark,
        src=input_cfg["transactions_path"],
        dst=f"{bronze_root}/transactions",
        ingestion_ts=ingestion_ts,
        label="transactions",
    )

    logger.info("Bronze ingestion complete.")


# ─────────────────────────────────────────────────────────────────────────────
# Private helpers
# ─────────────────────────────────────────────────────────────────────────────

def _ingest_csv(
    spark: SparkSession,
    src: str,
    dst: str,
    ingestion_ts: datetime,
    label: str,
) -> None:
    """
    Read a CSV file and write it to a Delta table as-is.

    inferSchema=false keeps every column as STRING so that the Bronze layer
    faithfully represents the raw text payload.  Quote and escape handling
    follow CSV defaults (double-quote char, backslash escape).
    """
    logger.info("[bronze] ingesting %s from %s", label, src)

    df = (
        spark.read
        .option("header",      "true")
        .option("inferSchema", "false")   # Bronze: preserve raw strings
        .option("multiLine",   "false")
        .csv(src)
    )

    df = df.withColumn(
        "ingestion_timestamp",
        F.lit(ingestion_ts).cast(TimestampType()),
    )

    logger.info("[bronze] %s schema: %s", label, df.schema.simpleString())

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .save(dst)
    )
    logger.info("[bronze] wrote %s → %s", label, dst)


def _ingest_jsonl(
    spark: SparkSession,
    src: str,
    dst: str,
    ingestion_ts: datetime,
    label: str,
) -> None:
    """
    Read a JSONL file (one JSON object per line) and write to Delta.

    Spark's JSON reader handles nested objects natively — the `location` and
    `metadata` sub-objects will be stored as StructType columns in Parquet.
    Schema inference reads the full file once; this is acceptable for up to
    ~2 GB given the 30-minute time limit.

    merchant_subcategory is absent from Stage 1 data entirely.  Spark will
    simply not include the column.  The Silver layer adds it as NULL when
    missing.  We do NOT add it here to remain faithful to the source.
    """
    logger.info("[bronze] ingesting %s from %s", label, src)

    df = (
        spark.read
        .option("multiLine", "false")   # JSONL: one object per line
        .json(src)
    )

    df = df.withColumn(
        "ingestion_timestamp",
        F.lit(ingestion_ts).cast(TimestampType()),
    )

    logger.info("[bronze] %s schema: %s", label, df.schema.simpleString())

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .save(dst)
    )
    logger.info("[bronze] wrote %s → %s", label, dst)
