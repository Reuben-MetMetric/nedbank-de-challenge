"""
Gold layer: build the scored dimensional model from Silver tables.

Output tables
─────────────
  {gold_path}/dim_customers/    — 9 fields (surrogate key + customer attributes)
  {gold_path}/dim_accounts/     — 11 fields (surrogate key; customer_ref → customer_id)
  {gold_path}/fact_transactions/— 15 fields (surrogate + FK keys, all measures)

Surrogate key strategy
──────────────────────
  sha2(natural_key, 256) → first 15 hex chars → conv base-16 → base-10 → BIGINT.

  Why sha2 instead of row_number():
  • row_number() OVER (ORDER BY natural_key) forces a global sort across all
    partitions, collapsing the entire table to a single Spark task — an O(N log N)
    bottleneck that is penalised by the efficiency scorer and will hit the wall-clock
    limit at Stage 2 scale (3 million transactions).
  • sha2 is computed per-row with no cross-partition coordination: O(N), fully
    parallelisable, and deterministic for the same input.
  • Value range: first 15 hex chars = 2^60 ≈ 1.15e18  <  BIGINT max 9.22e18. ✓
  • Collision probability for 1M rows ≈ (1M)² / 2^61 ≈ 4×10⁻⁷ — negligible. ✓
  • Stable across re-runs: same UUID input → same SHA-256 digest → same BIGINT. ✓

  Because the same _sha2_sk() function is applied to the same natural key in both
  the dim and the fact, foreign key values in fact_transactions are guaranteed to
  match primary keys in dim_accounts / dim_customers without re-reading the written
  Delta tables back into Spark. This eliminates 2 additional Delta reads from the
  Gold provisioning step.

Validation query mapping
────────────────────────
  Q1 — fact_transactions.transaction_type counts/amounts     (correct dedup + typing)
  Q2 — COUNT(*) FROM dim_accounts LEFT JOIN dim_customers
        WHERE dim_customers.customer_id IS NULL = 0          (GAP-026 fix: customer_id field)
  Q3 — dim_customers.province distribution via dim_accounts  (all 9 SA provinces present)

Age-band derivation (dim_customers)
────────────────────────────────────
  age  = FLOOR( datediff(current_date, dob) / 365.25 )
  band = 18-25 | 26-35 | 36-45 | 46-55 | 56-65 | 65+
  dob is NOT copied to the Gold output; age_band replaces it.
"""

import json
import logging
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import LongType

from pipeline.spark_utils import get_or_create_spark, load_config

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Surrogate key helper
# ─────────────────────────────────────────────────────────────────────────────

def _sha2_sk(natural_key_col) -> "Column":
    """
    Deterministic, parallelisable surrogate key from a natural key column.

    Algorithm: SHA-256(natural_key) → take first 15 hex chars
               → convert from base-16 to base-10 → cast to BIGINT.

    Range:  [0, 16^15) = [0, 2^60) ≈ 1.15e18  — fits in signed BIGINT (2^63-1).
    No Window / global sort required — computed per partition independently.
    """
    return F.conv(
        F.substring(F.sha2(natural_key_col.cast("string"), 256), 1, 15),
        16,   # from base: hexadecimal
        10,   # to   base: decimal
    ).cast(LongType())


# ─────────────────────────────────────────────────────────────────────────────
# Public entry point
# ─────────────────────────────────────────────────────────────────────────────

def run_provisioning(config: dict = None) -> None:
    """Build and write all Gold layer Delta tables."""
    if config is None:
        config = load_config()

    spark  = get_or_create_spark(config)
    silver = config["output"]["silver_path"]
    gold   = config["output"]["gold_path"]

    logger.info("Gold provisioning started.")

    # ── Read Silver tables ────────────────────────────────────────────────
    silver_customers    = spark.read.format("delta").load(f"{silver}/customers")
    silver_accounts     = spark.read.format("delta").load(f"{silver}/accounts")
    silver_transactions = spark.read.format("delta").load(f"{silver}/transactions")

    # ── Build dims ────────────────────────────────────────────────────────
    _build_dim_customers(silver_customers).write.format("delta").mode("overwrite").save(f"{gold}/dim_customers")
    logger.info("[gold] dim_customers written → %s/dim_customers", gold)

    _build_dim_accounts(silver_accounts).write.format("delta").mode("overwrite").save(f"{gold}/dim_accounts")
    logger.info("[gold] dim_accounts written → %s/dim_accounts", gold)

    # ── Build fact ────────────────────────────────────────────────────────
    # sha2 SKs are deterministic from natural keys, so account_sk and
    # customer_sk in the fact are guaranteed to match the dim PKs without
    # re-reading the written Delta tables.
    fact_df = _build_fact_transactions(
        silver_transactions,
        silver_accounts,
        silver_customers,
    )
    fact_df.write.format("delta").mode("overwrite").save(f"{gold}/fact_transactions")
    logger.info("[gold] fact_transactions written → %s/fact_transactions", gold)

    # ── Stage 2+: write dq_report.json ───────────────────────────────────
    dq_report_path = config["output"].get("dq_report_path")
    if dq_report_path:
        _write_dq_report(
            spark,
            gold_path=gold,
            dq_report_path=dq_report_path,
        )

    logger.info("Gold provisioning complete.")


# ─────────────────────────────────────────────────────────────────────────────
# Dimension builders
# ─────────────────────────────────────────────────────────────────────────────

def _age_band_col(dob_col) -> "Column":
    """
    Derive age_band from a DATE column using the pipeline run date.

    age = FLOOR( datediff(today, dob) / 365.25 )
    """
    age = F.floor(F.datediff(F.current_date(), dob_col) / F.lit(365.25))
    return (
        F.when(age >= 65, "65+")
        .when(age >= 56, "56-65")
        .when(age >= 46, "46-55")
        .when(age >= 36, "36-45")
        .when(age >= 26, "26-35")
        .when(age >= 18, "18-25")
        .otherwise(F.lit(None).cast("string"))
    )


def _build_dim_customers(silver_customers: DataFrame) -> DataFrame:
    """
    Build dim_customers (9 fields).

    Surrogate key: sha2-based deterministic hash of customer_id.
    age_band derived from dob; dob itself is NOT included in the output.
    """
    return silver_customers.select(
        _sha2_sk(F.col("customer_id")).alias("customer_sk"),
        "customer_id",
        "gender",
        "province",
        "income_band",
        "segment",
        "risk_score",   # already INT from Silver; no-op cast removed
        "kyc_status",
        _age_band_col(F.col("dob")).alias("age_band"),
        # dob intentionally excluded from Gold output per output_schema_spec §4
    )


def _build_dim_accounts(silver_accounts: DataFrame) -> DataFrame:
    """
    Build dim_accounts (11 fields).

    customer_ref is renamed to customer_id here (GAP-026 fix).
    Surrogate key: sha2-based deterministic hash of account_id.
    """
    return silver_accounts.select(
        _sha2_sk(F.col("account_id")).alias("account_sk"),
        "account_id",
        F.col("customer_ref").alias("customer_id"),   # rename per GAP-026
        "account_type",
        "account_status",
        "open_date",
        "product_tier",
        "digital_channel",
        "credit_limit",     # already DECIMAL(18,2) from Silver; no-op cast removed
        "current_balance",  # already DECIMAL(18,2) from Silver; no-op cast removed
        "last_activity_date",
    )


# ─────────────────────────────────────────────────────────────────────────────
# Fact builder
# ─────────────────────────────────────────────────────────────────────────────

def _build_fact_transactions(
    silver_tx: DataFrame,
    silver_accounts: DataFrame,
    silver_customers: DataFrame,
) -> DataFrame:
    """
    Build fact_transactions (15 fields).

    SK derivation
    ─────────────
    account_sk  = _sha2_sk(account_id)   — same formula as dim_accounts.account_sk
    customer_sk = _sha2_sk(customer_id)  — same formula as dim_customers.customer_sk
    transaction_sk = _sha2_sk(transaction_id)

    Because sha2 is a pure function of the natural key, the FK values computed
    here are bit-for-bit identical to the PK values written into the dim tables —
    no re-read of the written Delta tables is required.

    Join strategy
    ─────────────
    1. Build a slim bridge from Silver accounts: account_id → account_sk,
       customer_ref (→ customer_id bridge).
    2. Build a slim bridge from Silver customers: customer_id → customer_sk.
    3. Inner-join Silver transactions → account bridge (drops orphans — they
       cannot satisfy fact_transactions.account_sk NOT NULL).
    4. Inner-join result → customer bridge to resolve customer_sk.
    5. Both dim bridges are broadcast-hinted (small tables).
    """

    # Slim bridge: account → (account_sk, customer_ref)
    acc_bridge = (
        silver_accounts
        .select(
            F.col("account_id").alias("_acc_id"),
            _sha2_sk(F.col("account_id")).alias("account_sk"),
            F.col("customer_ref").alias("_cust_ref"),
        )
    )

    # Slim bridge: customer → customer_sk
    cust_bridge = (
        silver_customers
        .select(
            F.col("customer_id").alias("_cust_id"),
            _sha2_sk(F.col("customer_id")).alias("customer_sk"),
        )
    )

    # Step 1: resolve account_sk and bridging customer_ref
    fact = silver_tx.join(
        F.broadcast(acc_bridge),
        silver_tx["account_id"] == acc_bridge["_acc_id"],
        "inner",   # drops orphaned transactions
    ).drop("_acc_id")

    # Step 2: resolve customer_sk via the bridging customer_ref
    fact = fact.join(
        F.broadcast(cust_bridge),
        fact["_cust_ref"] == cust_bridge["_cust_id"],
        "inner",
    ).drop("_cust_ref", "_cust_id")

    # Step 3: transaction_sk — per-row hash, no sort needed
    fact = fact.withColumn("transaction_sk", _sha2_sk(F.col("transaction_id")))

    # Step 4: select the 15 scored fields in schema-spec order
    return fact.select(
        "transaction_sk",
        "transaction_id",
        "account_sk",
        "customer_sk",
        "transaction_date",
        "transaction_timestamp",
        "transaction_type",
        "merchant_category",
        "merchant_subcategory",
        "amount",
        "currency",
        "channel",
        "province",
        "dq_flag",
        "ingestion_timestamp",
    )


# ─────────────────────────────────────────────────────────────────────────────
# DQ report (Stage 2+)
# ─────────────────────────────────────────────────────────────────────────────

def _write_dq_report(
    spark: SparkSession,
    gold_path: str,
    dq_report_path: str,
) -> None:
    """
    Write a JSON DQ summary report to /data/output/dq_report.json.

    Counts flagged records per dq_flag code from the Gold fact_transactions
    table so the report and the table are always in sync.
    """
    fact = spark.read.format("delta").load(f"{gold_path}/fact_transactions")

    # Single-scan: groupBy(dq_flag) groups NULL rows too, giving us both
    # the clean-record count and per-flag counts in one shuffle pass.
    grouped = fact.groupBy("dq_flag").count().collect()

    total_rows    = sum(row["count"] for row in grouped)
    flag_counts   = {row["dq_flag"]: row["count"] for row in grouped
                     if row["dq_flag"] is not None}
    total_flagged = sum(flag_counts.values())

    report = {
        "pipeline_run_timestamp": datetime.now(timezone.utc).isoformat(),
        "total_transactions": total_rows,
        "total_flagged":      total_flagged,
        "flag_breakdown":     flag_counts,
    }

    with open(dq_report_path, "w") as fh:
        json.dump(report, fh, indent=2)

    logger.info(
        "[gold] dq_report written → %s  total_flagged=%d",
        dq_report_path,
        total_flagged,
    )
