"""
Shared SparkSession factory and config loader.

Centralising these here means ingest, transform and provision all share one
session (and one JVM) and read config from the same place.
"""

import os
import logging

import yaml
from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)

# Scoring system may override the config path via this env var.
_DEFAULT_CONFIG_PATH = "/data/config/pipeline_config.yaml"


def load_config(path: str = None) -> dict:
    """Load and return the YAML pipeline configuration."""
    resolved = path or os.environ.get("PIPELINE_CONFIG", _DEFAULT_CONFIG_PATH)
    logger.info("Loading config from: %s", resolved)
    with open(resolved) as fh:
        return yaml.safe_load(fh)


_DEFAULT_DQ_RULES_PATH = "/data/config/dq_rules.yaml"
_BUNDLED_DQ_RULES_PATH = "/app/config/dq_rules.yaml"


def load_dq_rules(path: str = None) -> dict:
    """Load and return the YAML data-quality rules configuration.

    Resolution order:
      1. Explicit ``path`` argument
      2. ``DQ_RULES_PATH`` environment variable
      3. ``/data/config/dq_rules.yaml`` (scoring-system mount, if present)
      4. ``/app/config/dq_rules.yaml`` (bundled fallback)
    """
    resolved = path or os.environ.get("DQ_RULES_PATH")
    if not resolved:
        resolved = (
            _DEFAULT_DQ_RULES_PATH
            if os.path.exists(_DEFAULT_DQ_RULES_PATH)
            else _BUNDLED_DQ_RULES_PATH
        )
    logger.info("Loading DQ rules from: %s", resolved)
    with open(resolved) as fh:
        return yaml.safe_load(fh)


def get_or_create_spark(config: dict) -> SparkSession:
    """
    Return an existing SparkSession or create one configured for the
    2 vCPU / 2 GB RAM scoring environment.

    Delta Lake extensions are registered once here; subsequent
    getOrCreate() calls in the same JVM will reuse this session.
    """
    spark_cfg = config.get("spark", {})
    master   = spark_cfg.get("master",   "local[2]")
    app_name = spark_cfg.get("app_name", "nedbank-de-pipeline")

    # Delta Lake JARs are bundled in /app/jars/ — no Maven/Ivy download needed.
    # This makes the build reproducible in an offline scoring environment.
    _jars_dir = "/app/jars"
    _delta_jars = ",".join([
        f"{_jars_dir}/delta-spark_2.12-3.1.0.jar",
        f"{_jars_dir}/delta-storage-3.1.0.jar",
        f"{_jars_dir}/antlr4-runtime-4.9.3.jar",
    ])

    builder = (
        SparkSession.builder
        .master(master)
        .appName(app_name)
        # ── Heap sizing — must fit within 2 GB container ceiling ──────────
        .config("spark.driver.memory",   "512m")
        .config("spark.executor.memory", "1g")
        # ── Parallelism — match the 2-vCPU constraint ─────────────────────
        .config("spark.default.parallelism",       "4")
        .config("spark.sql.shuffle.partitions",    "4")
        # ── Delta Lake JARs loaded directly — no Ivy/Maven resolution ─────
        .config("spark.jars", _delta_jars)
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        # ── Temp dir — /tmp is the only writable tmpfs in the scoring container ─
        # spark.local.dir: Spark shuffle/spill files
        # java.io.tmpdir:  native lib extraction (read-only filesystem)
        # parquet codec:   uncompressed avoids snappy native lib (fails with
        #                  --read-only + --cap-drop=ALL in scoring environment)
        .config("spark.local.dir", "/tmp")
        .config("spark.driver.extraJavaOptions",   "-Djava.io.tmpdir=/tmp")
        .config("spark.executor.extraJavaOptions", "-Djava.io.tmpdir=/tmp")
        .config("spark.sql.parquet.compression.codec", "uncompressed")
        # ── Reduce verbose Spark logging in scored runs ───────────────────
        .config("spark.ui.enabled", "false")
    )

    session = builder.getOrCreate()
    session.sparkContext.setLogLevel("WARN")
    return session
