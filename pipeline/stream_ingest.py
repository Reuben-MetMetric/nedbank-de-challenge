"""
Stage 3 stub: streaming ingestion path.

Implement in Stage 3 (days 15–21).  The batch pipeline (run_all.py) does not
call this module; it is invoked separately when Stage 3 is evaluated.

See docs/stage3_spec_addendum.md and docs/stream_interface_spec.md for the
full streaming specification.
"""

import logging

logger = logging.getLogger(__name__)


def run_stream_ingestion(config: dict = None) -> None:
    """
    Ingest micro-batch JSONL stream files from /data/stream/ into
    stream_gold/current_balances and stream_gold/recent_transactions.

    TODO (Stage 3):
      1. Load config and get stream_input_path, stream_gold_path.
      2. Poll /data/stream/ for new .jsonl batch files (poll_interval_seconds).
      3. For each new file, read and parse transactions (same schema as batch).
      4. Upsert into stream_gold/current_balances (balance per account_id).
      5. Upsert into stream_gold/recent_transactions (last 50 per account_id).
      6. Exit when no new files arrive for quiesce_timeout_seconds.
    """
    logger.info("stream_ingest is not yet implemented (Stage 3).")
    raise NotImplementedError("Stage 3 streaming not yet implemented.")
