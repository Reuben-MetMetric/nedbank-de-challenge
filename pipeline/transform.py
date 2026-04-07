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

from pipeline.spark_utils import get_or_create_spark, load_config, load_dq_rules

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# Currency normalisation helpers  (config-driven via dq_rules.yaml)
# ─────────────────────────────────────────────────────────────────────────────

def _is_zar_variant(currency_col, curr_cfg: dict) -> "Column":
    """True when the raw currency value is a non-target-value variant."""
    target      = curr_cfg["target_value"]
    variants    = [target] + curr_cfg["known_variants"]
    iso_numeric = curr_cfg["iso_numeric"]
    return (
        ~currency_col.isin(target)
        & (
            currency_col.isin(*variants)
            | (currency_col.cast("int") == iso_numeric)
        )
    )


def _normalise_currency(currency_col, curr_cfg: dict) -> "Column":
    """Return the target currency code for all known variants; preserve others."""
    target      = curr_cfg["target_value"]
    variants    = [target] + curr_cfg["known_variants"]
    iso_numeric = curr_cfg["iso_numeric"]
    return (
        F.when(
            currency_col.isin(*variants)
            | (currency_col.cast("int") == iso_numeric),
            F.lit(target),
        ).otherwise(currency_col)
    )


# ─────────────────────────────────────────────────────────────────────────────
# Date parsing helpers  (config-driven via dq_rules.yaml)
# ─────────────────────────────────────────────────────────────────────────────

def _parse_date_primary(col_expr, date_cfg: dict) -> "Column":
    """Parse date using the primary ISO format from config."""
    return F.to_date(col_expr, date_cfg["primary_format"])


def _parse_date_any(col_expr, date_cfg: dict):
    """
    Try primary ISO format, then variant formats, then Unix epoch.
    All formats are sourced from date_format_checks in dq_rules.yaml.
    Returns a DATE column (nulls where all attempts fail).
    """
    primary_fmt   = date_cfg["primary_format"]
    variant_fmts  = date_cfg.get("variant_formats", [])
    epoch_pattern = date_cfg.get("epoch_pattern", r"^\d{9,11}$")

    result = F.to_date(col_expr, primary_fmt)
    for fmt in variant_fmts:
        result = F.coalesce(result, F.to_date(col_expr, fmt))
    # Epoch integers stored as strings
    result = F.coalesce(
        result,
        F.when(
            col_expr.rlike(epoch_pattern),
            F.to_date(F.from_unixtime(col_expr.cast("long"))),
        ),
    )
    return result




# ─────────────────────────────────────────────────────────────────────────────
# Public entry point
# ─────────────────────────────────────────────────────────────────────────────

def run_transformation(config: dict = None) -> None:
    """Transform all three Bronze tables into typed, clean Silver tables."""
    if config is None:
        config = load_config()
    dq_rules = load_dq_rules()

    spark  = get_or_create_spark(config)
    bronze = config["output"]["bronze_path"]
    silver = config["output"]["silver_path"]

    logger.info("Silver transformation started.")

    # Order matters: accounts must be written before transactions so the
    # orphan-account cross-reference JOIN can read from Silver accounts.
    _transform_customers(spark, f"{bronze}/customers",    f"{silver}/customers",   dq_rules)
    _transform_accounts( spark, f"{bronze}/accounts",     f"{silver}/accounts",    dq_rules)
    _transform_transactions(
        spark,
        bronze_src=f"{bronze}/transactions",
        silver_accounts_path=f"{silver}/accounts",
        silver_dst=f"{silver}/transactions",
        dq_rules=dq_rules,
    )

    logger.info("Silver transformation complete.")


# ─────────────────────────────────────────────────────────────────────────────
# Per-table transforms
# ─────────────────────────────────────────────────────────────────────────────

def _transform_customers(
    spark: SparkSession,
    bronze_src: str,
    silver_dst: str,
    dq_rules: dict,
) -> None:
    logger.info("[silver] transforming customers")
    date_cfg = dq_rules["date_format_checks"]

    df: DataFrame = spark.read.format("delta").load(bronze_src)

    # ── Deduplication (keep one row per customer_id) ──────────────────────
    # dropDuplicates is hash-based — no Window sort, no shuffle beyond group-by.
    # The spec doesn’t require which duplicate is kept for customers.
    df = df.dropDuplicates(["customer_id"])

    df = df.select(
        "customer_id",
        "id_number",
        "first_name",
        "last_name",
        _parse_date_any(F.col("dob"), date_cfg).alias("dob"),
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
    dq_rules: dict,
) -> None:
    logger.info("[silver] transforming accounts")
    date_cfg    = dq_rules["date_format_checks"]
    null_fields = dq_rules["null_checks"]["accounts"]["fields"]

    df: DataFrame = spark.read.format("delta").load(bronze_src)

    # ── Drop records with null required fields (NULL_REQUIRED) ────────────
    for field in null_fields:
        df = df.filter(F.col(field).isNotNull())

    # ── Deduplication ──────────────────────────────────────────────────────
    # Hash-based dropDuplicates — avoids Window sort overhead.
    # The spec doesn’t require which duplicate is retained for accounts.
    df = df.dropDuplicates(["account_id"])

    df = df.select(
        "account_id",
        "customer_ref",
        "account_type",
        "account_status",
        _parse_date_any(F.col("open_date"), date_cfg).alias("open_date"),
        "product_tier",
        "mobile_number",
        "digital_channel",
        F.col("credit_limit").cast(DecimalType(18, 2)).alias("credit_limit"),
        F.col("current_balance").cast(DecimalType(18, 2)).alias("current_balance"),
        _parse_date_any(F.col("last_activity_date"), date_cfg).alias("last_activity_date"),
        "ingestion_timestamp",
    )

    df.write.format("delta").mode("overwrite").save(silver_dst)
    logger.info("[silver] accounts written → %s", silver_dst)


def _transform_transactions(
    spark: SparkSession,
    bronze_src: str,
    silver_accounts_path: str,
    silver_dst: str,
    dq_rules: dict,
) -> None:
    logger.info("[silver] transforming transactions")
    curr_cfg    = dq_rules["currency_normalisation"]
    date_cfg    = dq_rules["date_format_checks"]
    null_fields = dq_rules["null_checks"]["transactions"]["fields"]
    dup_cfg     = dq_rules["duplicate_checks"]["transactions"]
    orphan_cfg  = dq_rules["orphan_checks"]["transactions"]

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
    # Materialise the primary-format parse as a column FIRST.
    # Both transaction_date_parsed and date_is_variant need it; materialising
    # avoids evaluating to_date(col, primary_fmt) twice per row in the
    # combined dq_flag expression (which would re-invoke it inside
    # _date_is_variant).  With 1M+ rows this is a measurable saving.
    primary_fmt = date_cfg["primary_format"]
    df = df.withColumn("_tx_date_primary",
                       F.to_date(F.col("transaction_date"), primary_fmt))
    # Full parse: _tx_date_primary coalesced with variant formats + epoch.
    variant_fmts  = date_cfg.get("variant_formats", [])
    epoch_pattern = date_cfg.get("epoch_pattern", r"^\d{9,11}$")
    tx_date_full  = F.col("_tx_date_primary")
    for fmt in variant_fmts:
        tx_date_full = F.coalesce(tx_date_full, F.to_date(F.col("transaction_date"), fmt))
    tx_date_full = F.coalesce(
        tx_date_full,
        F.when(F.col("transaction_date").rlike(epoch_pattern),
               F.to_date(F.from_unixtime(F.col("transaction_date").cast("long")))),
    )
    df = df.withColumn("transaction_date_parsed", tx_date_full)
    # Variant flag: primary returned null but full parse succeeded.
    # Uses already-materialised columns — no re-evaluation of to_date.
    date_is_variant = (
        F.col("_tx_date_primary").isNull()
        & F.col("transaction_date_parsed").isNotNull()
    )

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
    is_currency_variant = _is_zar_variant(currency_col, curr_cfg)
    df = df.withColumn("currency", _normalise_currency(currency_col, curr_cfg))

    # ── NULL_REQUIRED check (required fields from dq_rules.yaml) ─────────
    null_required = F.lit(False)
    for _f in null_fields:
        null_required = null_required | F.col(_f).isNull()

    # ── Deduplication (natural key + tiebreak order from dq_rules.yaml) ──
    # Keep the earliest record (by tiebreak cols) within each duplicate group.
    # The survivor is flagged DUPLICATE_DEDUPED if the group had >1 member.
    #
    # Both window specs share the same PARTITION BY + ORDER BY, allowing
    # Spark to combine them into a single sort pass in the physical plan.
    # Using rowsBetween(unboundedPreceding, unboundedFollowing) on the count
    # window produces the full-group count (not a running sum).
    natural_key    = dup_cfg["natural_key"]
    tiebreak_order = dup_cfg["tiebreak_order"]
    w_dup   = Window.partitionBy(natural_key).orderBy(*tiebreak_order)
    w_frame = (
        Window.partitionBy(natural_key)
        .orderBy(*tiebreak_order)
        .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    )
    df = (
        df.withColumn("_rn",    F.row_number().over(w_dup))
          .withColumn("_total", F.count("*").over(w_frame))
    )
    # Materialise the duplicate flag BEFORE filtering rows away.
    df = df.withColumn("_is_duplicate", F.col("_total") > 1)
    df = df.filter(F.col("_rn") == 1).drop("_rn", "_total")

    # ── Orphaned account check (join key from dq_rules.yaml) ─────────────
    # Left-join against Silver accounts; a miss means ORPHANED_ACCOUNT.
    orphan_join_key = orphan_cfg["join_key"]
    silver_acc = (
        spark.read.format("delta").load(silver_accounts_path)
        .select(F.col(orphan_join_key).alias("_valid_account_id"))
    )
    df = df.join(
        F.broadcast(silver_acc),
        df[orphan_join_key] == silver_acc["_valid_account_id"],
        "left",
    )
    # Materialise the orphan flag NOW, before _valid_account_id is dropped.
    df = df.withColumn("_is_orphan", F.col("_valid_account_id").isNull())
    df = df.drop("_valid_account_id")

    # ── Assemble dq_flag (first matching condition wins) ──────────────────
    type_flag_code = dq_rules["type_checks"]["transactions"][0]["flag_code"]
    dq_flag = (
        F.when(null_required,           F.lit("NULL_REQUIRED"))
        .when(F.col("_is_orphan"),      F.lit(orphan_cfg["flag_code"]))
        .when(F.col("_is_duplicate"),   F.lit(dup_cfg["flag_code"]))
        .when(date_is_variant,          F.lit(date_cfg["flag_code"]))
        .when(is_currency_variant,      F.lit(curr_cfg["flag_code"]))
        .when(amount_type_mismatch,     F.lit(type_flag_code))
        .otherwise(F.lit(None).cast("string"))
    )
    df = df.withColumn("dq_flag", dq_flag).drop("_is_orphan", "_is_duplicate")

    # ── Final column selection ─────────────────────────────────────────────
    # _tx_date_primary is excluded (not in final schema).
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
