"""
Shared SparkSession factory and config loader.

Centralising these here means ingest, transform and provision all share one
session (and one JVM) and read config from the same place.
"""

import os
import logging

import yaml
from delta import configure_spark_with_delta_pip
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

    builder = (
        SparkSession.builder
        .master(master)
        .appName(app_name)
        # ── Heap sizing — must fit within 2 GB container ceiling ──────────
        .config("spark.driver.memory",   "1g")
        .config("spark.executor.memory", "1g")
        # ── Parallelism — match the 2-vCPU constraint ─────────────────────
        .config("spark.default.parallelism",       "4")
        .config("spark.sql.shuffle.partitions",    "4")
        # ── Delta Lake extensions ─────────────────────────────────────────
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        # ── Temp dir — /tmp is a 512 MB tmpfs in the scoring container ────
        .config("spark.local.dir", "/tmp")
        # ── Reduce verbose Spark logging in scored runs ───────────────────
        .config("spark.ui.enabled", "false")
    )

    # configure_spark_with_delta_pip adds the pip-installed Delta Lake JARs
    # to the Spark driver classpath — required when delta-spark is installed
    # via pip rather than added as a Maven package at session start.
    session = configure_spark_with_delta_pip(builder).getOrCreate()
    session.sparkContext.setLogLevel("WARN")
    return session
