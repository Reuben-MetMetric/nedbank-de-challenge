"""
Silver layer: clean, conform, and DQ-flag Bronze tables.

Transformations applied per table
──────────────────────────────────
customers
  • Deduplicate on customer_id (keep first by stable natural-key order)
  • Parse dob → DATE (multi-format aware for Stage 2)
  • Cast risk_score → INT

accounts
  • Drop rows with null account_id (NULL_REQUIRED; Stage 2 ~0.5%)
  • Deduplicate on account_id
  • Parse open_date, last_activity_date → DATE (multi-format)
  • Cast credit_limit, current_balance → DECIMAL(18,2)

transactions
  • Handle absent merchant_subcategory (Stage 1 data never includes it)
  • Cast amount → DECIMAL(18,2); flag TYPE_MISMATCH on failure
  • Parse transaction_date → DATE (multi-format); flag DATE_FORMAT on variant
  • Combine transaction_date + transaction_time → TIMESTAMP
  • Normalise currency to "ZAR"; flag CURRENCY_VARIANT on variant
  • Cross-reference accounts; flag ORPHANED_ACCOUNT on miss
  • Deduplicate on transaction_id; flag DUPLICATE_DEDUPED on survivor of a dup set
  • Flatten location / metadata StructType columns to top-level fields
  • Write dq_flag: NULL for clean records; one of six issue codes otherwise

DQ flag priority (first match wins)
  1. NULL_REQUIRED      — transaction_id, account_id, amount, or transaction_type is null
  2. ORPHANED_ACCOUNT   — account_id not found in Silver accounts
  3. DUPLICATE_DEDUPED  — kept survivor from a duplicate transaction_id group
  4. DATE_FORMAT        — transaction_date parsed via non-primary format
  5. CURRENCY_VARIANT   — currency was not literally "ZAR"
  6. TYPE_MISMATCH      — amount could not be cast to DECIMAL(18,2)

Stage notes
───────────
  Stage 1 — data is clean, all dq_flag values will be NULL.
  Stage 2 — all six flag types can fire; DQ rules are externalised in
            config/dq_rules.yaml so thresholds and rules can change without
            touching this module.
  Stage 3 — same rules apply; streaming Silver re-uses _transform_transactions.
"""

import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType
from pyspark.sql.window import Window

from pipeline.spark_utils import get_or_create_spark, load_config

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# Currency normalisation helpers
# ─────────────────────────────────────────────────────────────────────────────

# All known representations of South African Rand in Stage 1/2 source data.
_ZAR_STRINGS = ("ZAR", "zar", "R", "rands", "Rands", "RANDS")
_ZAR_ISO_NUMERIC = 710  # ISO 4217 numeric code for ZAR


def _is_zar_variant(currency_col) -> "Column":
    """True when the raw currency value is a non-"ZAR" variant."""
    return (
        ~currency_col.isin("ZAR")
        & (
            currency_col.isin(*_ZAR_STRINGS)
            | (currency_col.cast("int") == _ZAR_ISO_NUMERIC)
        )
    )


def _normalise_currency(currency_col) -> "Column":
    """Return "ZAR" for all known SA Rand representations; preserve others."""
    return (
        F.when(
            currency_col.isin(*_ZAR_STRINGS)
            | (currency_col.cast("int") == _ZAR_ISO_NUMERIC),
            F.lit("ZAR"),
        ).otherwise(currency_col)
    )


# ─────────────────────────────────────────────────────────────────────────────
# Date parsing helpers
# ─────────────────────────────────────────────────────────────────────────────

# Primary ISO format used throughout Stage 1 data.
_PRIMARY_DATE_FMT = "yyyy-MM-dd"

# Stage 2 variant formats announced in the spec addendum.
_VARIANT_DATE_FMTS = ("dd/MM/yyyy",)

# Unix epoch pattern: 9–11 digit integer string (covers 2001 – 2286)
_EPOCH_PATTERN = r"^\d{9,11}$"


def _parse_date_primary(col_expr) -> "Column":
    """Parse date using the primary ISO format only."""
    return F.to_date(col_expr, _PRIMARY_DATE_FMT)


def _parse_date_any(col_expr):
    """
    Try primary ISO format, then variant formats, then Unix epoch.
    Returns a DATE column (nulls where all attempts fail).
    """
    result = F.to_date(col_expr, _PRIMARY_DATE_FMT)
    for fmt in _VARIANT_DATE_FMTS:
        result = F.coalesce(result, F.to_date(col_expr, fmt))
    # Epoch integers stored as strings
    result = F.coalesce(
        result,
        F.when(
            col_expr.rlike(_EPOCH_PATTERN),
            F.to_date(F.from_unixtime(col_expr.cast("long"))),
        ),
    )
    return result


def _date_is_variant(col_expr) -> "Column":
    """
    True when the raw string represents a valid date but NOT in primary
    ISO format — i.e., it required a fallback parse.
    """
    primary_ok  = _parse_date_primary(col_expr).isNotNull()
    fallback_ok = _parse_date_any(col_expr).isNotNull()
    return ~primary_ok & fallback_ok


# ─────────────────────────────────────────────────────────────────────────────
# Public entry point
# ─────────────────────────────────────────────────────────────────────────────

def run_transformation(config: dict = None) -> None:
    """Transform all three Bronze tables into typed, clean Silver tables."""
    if config is None:
        config = load_config()

    spark  = get_or_create_spark(config)
    bronze = config["output"]["bronze_path"]
    silver = config["output"]["silver_path"]

    logger.info("Silver transformation started.")

    # Order matters: accounts must be written before transactions so the
    # orphan-account cross-reference JOIN can read from Silver accounts.
    _transform_customers(spark, f"{bronze}/customers",    f"{silver}/customers")
    _transform_accounts( spark, f"{bronze}/accounts",     f"{silver}/accounts")
    _transform_transactions(
        spark,
        bronze_src=f"{bronze}/transactions",
        silver_accounts_path=f"{silver}/accounts",
        silver_dst=f"{silver}/transactions",
    )

    logger.info("Silver transformation complete.")


# ─────────────────────────────────────────────────────────────────────────────
# Per-table transforms
# ─────────────────────────────────────────────────────────────────────────────

def _transform_customers(
    spark: SparkSession,
    bronze_src: str,
    silver_dst: str,
) -> None:
    logger.info("[silver] transforming customers")

    df: DataFrame = spark.read.format("delta").load(bronze_src)

    # ── Deduplication (keep one row per customer_id) ──────────────────────
    w_dedup = Window.partitionBy("customer_id").orderBy("customer_id")
    df = (
        df.withColumn("_rn", F.row_number().over(w_dedup))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    df = df.select(
        "customer_id",
        "id_number",
        "first_name",
        "last_name",
        _parse_date_any(F.col("dob")).alias("dob"),
        "gender",
        "province",
        "income_band",
        "segment",
        F.col("risk_score").cast("int").alias("risk_score"),
        "kyc_status",
        "product_flags",
        "ingestion_timestamp",
    )

    df.write.format("delta").mode("overwrite").save(silver_dst)
    logger.info("[silver] customers written → %s", silver_dst)


def _transform_accounts(
    spark: SparkSession,
    bronze_src: str,
    silver_dst: str,
) -> None:
    logger.info("[silver] transforming accounts")

    df: DataFrame = spark.read.format("delta").load(bronze_src)

    # ── Drop records with null primary key (NULL_REQUIRED) ────────────────
    df = df.filter(F.col("account_id").isNotNull())

    # ── Deduplication ──────────────────────────────────────────────────────
    w_dedup = Window.partitionBy("account_id").orderBy("account_id")
    df = (
        df.withColumn("_rn", F.row_number().over(w_dedup))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    df = df.select(
        "account_id",
        "customer_ref",
        "account_type",
        "account_status",
        _parse_date_any(F.col("open_date")).alias("open_date"),
        "product_tier",
        "mobile_number",
        "digital_channel",
        F.col("credit_limit").cast(DecimalType(18, 2)).alias("credit_limit"),
        F.col("current_balance").cast(DecimalType(18, 2)).alias("current_balance"),
        _parse_date_any(F.col("last_activity_date")).alias("last_activity_date"),
        "ingestion_timestamp",
    )

    df.write.format("delta").mode("overwrite").save(silver_dst)
    logger.info("[silver] accounts written → %s", silver_dst)


def _transform_transactions(
    spark: SparkSession,
    bronze_src: str,
    silver_accounts_path: str,
    silver_dst: str,
) -> None:
    logger.info("[silver] transforming transactions")

    df: DataFrame = spark.read.format("delta").load(bronze_src)

    # ── Gracefully add merchant_subcategory if absent (Stage 1 data) ──────
    if "merchant_subcategory" not in df.columns:
        df = df.withColumn("merchant_subcategory", F.lit(None).cast("string"))

    # ── Flatten nested location / metadata structs ────────────────────────
    # Access nested fields before any renaming to avoid ambiguity.
    df = df.withColumn("province",    F.col("location.province")) \
           .withColumn("city",        F.col("location.city")) \
           .withColumn("coordinates", F.col("location.coordinates")) \
           .withColumn("device_id",   F.col("metadata.device_id")) \
           .withColumn("session_id",  F.col("metadata.session_id")) \
           .withColumn("retry_flag",  F.col("metadata.retry_flag")) \
           .drop("location", "metadata")

    # ── Type-cast amount (handles string amounts injected in Stage 2) ─────
    # Attempt cast first; a null result means TYPE_MISMATCH.
    df = df.withColumn(
        "amount_cast",
        F.col("amount").cast(DecimalType(18, 2)),
    )
    amount_type_mismatch = (
        F.col("amount").isNotNull() & F.col("amount_cast").isNull()
    )

    # ── Date parsing ──────────────────────────────────────────────────────
    df = df.withColumn(
        "transaction_date_parsed",
        _parse_date_any(F.col("transaction_date")),
    )
    date_is_variant = _date_is_variant(F.col("transaction_date"))

    # ── Build transaction_timestamp (DATE + TIME → TIMESTAMP) ─────────────
    # Use the raw strings before they are cast so the concat is reliable.
    df = df.withColumn(
        "transaction_timestamp",
        F.to_timestamp(
            F.concat(
                F.col("transaction_date"),
                F.lit(" "),
                F.col("transaction_time"),
            ),
            "yyyy-MM-dd HH:mm:ss",
        ),
    )

    # ── Currency variant detection and normalisation ──────────────────────
    currency_col = F.col("currency")
    is_currency_variant = _is_zar_variant(currency_col)
    df = df.withColumn("currency", _normalise_currency(currency_col))

    # ── NULL_REQUIRED check for non-nullable transaction fields ───────────
    null_required = (
        F.col("transaction_id").isNull()
        | F.col("account_id").isNull()
        | F.col("amount").isNull()
        | F.col("transaction_type").isNull()
    )

    # ── Deduplication on transaction_id ──────────────────────────────────
    # Within each duplicate group, keep the earliest record by date+time
    # (deterministic tie-breaking).  The survivor retains DUPLICATE_DEDUPED
    # if the group had more than one member.
    w_dup = Window.partitionBy("transaction_id").orderBy(
        "transaction_date", "transaction_time"
    )
    w_dup_count = Window.partitionBy("transaction_id")
    df = (
        df.withColumn("_rn",       F.row_number().over(w_dup))
          .withColumn("_dup_count", F.count("*").over(w_dup_count))
    )
    # Materialise the duplicate flag NOW, before _dup_count is dropped.
    df = df.withColumn("_is_duplicate", F.col("_dup_count") > 1)
    df = df.filter(F.col("_rn") == 1).drop("_rn", "_dup_count")

    # ── Orphaned account check ────────────────────────────────────────────
    # Left-join against Silver accounts; a miss means ORPHANED_ACCOUNT.
    silver_acc = (
        spark.read.format("delta").load(silver_accounts_path)
        .select(F.col("account_id").alias("_valid_account_id"))
    )
    df = df.join(
        F.broadcast(silver_acc),
        df["account_id"] == silver_acc["_valid_account_id"],
        "left",
    )
    # Materialise the orphan flag NOW, before _valid_account_id is dropped.
    df = df.withColumn("_is_orphan", F.col("_valid_account_id").isNull())
    df = df.drop("_valid_account_id")

    # ── Assemble dq_flag (first matching condition wins) ──────────────────
    dq_flag = (
        F.when(null_required,           F.lit("NULL_REQUIRED"))
        .when(F.col("_is_orphan"),      F.lit("ORPHANED_ACCOUNT"))
        .when(F.col("_is_duplicate"),   F.lit("DUPLICATE_DEDUPED"))
        .when(date_is_variant,          F.lit("DATE_FORMAT"))
        .when(is_currency_variant,      F.lit("CURRENCY_VARIANT"))
        .when(amount_type_mismatch,     F.lit("TYPE_MISMATCH"))
        .otherwise(F.lit(None).cast("string"))
    )
    df = df.withColumn("dq_flag", dq_flag).drop("_is_orphan", "_is_duplicate")

    # ── Final column selection ─────────────────────────────────────────────
    df = df.select(
        "transaction_id",
        "account_id",
        F.col("transaction_date_parsed").alias("transaction_date"),
        "transaction_time",
        "transaction_timestamp",
        "transaction_type",
        "merchant_category",
        "merchant_subcategory",
        F.col("amount_cast").alias("amount"),
        "currency",
        "channel",
        "province",
        "city",
        "coordinates",
        "device_id",
        "session_id",
        "retry_flag",
        "dq_flag",
        "ingestion_timestamp",
    )

    df.write.format("delta").mode("overwrite").save(silver_dst)
    logger.info("[silver] transactions written → %s", silver_dst)
