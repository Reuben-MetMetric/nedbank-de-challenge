"""
Pipeline entry point — orchestrates Bronze → Silver → Gold in sequence.

Invoked by the Docker container:
    python pipeline/run_all.py

Exit codes:
    0  — pipeline completed successfully
    1  — pipeline failed (exception or assertion); details in stderr/logs
"""

import logging
import sys
import traceback

from pipeline.ingest    import run_ingestion
from pipeline.transform import run_transformation
from pipeline.provision import run_provisioning
from pipeline.spark_utils import load_config

# ── Logging to stdout so Docker / scoring harness captures it ──────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("run_all")


def main() -> None:
    logger.info("=== Nedbank DE Challenge — pipeline starting ===")

    config = load_config()

    steps = [
        ("Bronze ingestion",         lambda: run_ingestion(config)),
        ("Silver transformation",    lambda: run_transformation(config)),
        ("Gold provisioning",        lambda: run_provisioning(config)),
    ]

    for step_name, step_fn in steps:
        logger.info("── Starting: %s", step_name)
        step_fn()
        logger.info("── Completed: %s", step_name)

    logger.info("=== Pipeline finished successfully ===")


if __name__ == "__main__":
    try:
        main()
        sys.exit(0)
    except Exception:  # noqa: BLE001
        logger.error("Pipeline failed:\n%s", traceback.format_exc())
        sys.exit(1)
