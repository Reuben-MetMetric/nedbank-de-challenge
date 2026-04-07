# Nedbank N*ovation Data Engineering Challenge — Stage 1

Medallion pipeline: **Bronze → Silver → Gold** using PySpark + Delta Lake, containerised with Docker.

---

## Architecture

```
/data/input/
  accounts.csv          → Bronze (raw string schema)
  customers.csv         → Bronze (raw string schema)
  transactions.jsonl    → Bronze (natural Spark schema)
          │
          ▼  pipeline/transform.py
/data/output/silver/
  accounts/             type-cast, deduped, DQ-flagged
  customers/            type-cast, deduped, DQ-flagged
  transactions/         type-cast, deduped, DQ-flagged, orphan-checked
          │
          ▼  pipeline/provision.py
/data/output/gold/
  dim_customers/        age_band derived from dob, sha2 surrogate key
  dim_accounts/         linked to customers via customer_id
  fact_transactions/    type+amount+dates, FK to dim_accounts & dim_customers
/data/output/dq_report.json
```

---

## Pipeline Modules

| Module | Layer | Purpose |
|--------|-------|---------|
| `pipeline/ingest.py` | Bronze | Raw ingest — CSV all-string, JSONL natural schema; adds `ingestion_timestamp` |
| `pipeline/transform.py` | Silver | Type casting, multi-format date parsing, currency normalisation, deduplication, DQ flagging |
| `pipeline/provision.py` | Gold | Dimensional model with deterministic sha2 surrogate keys; DQ report |
| `pipeline/spark_utils.py` | Shared | SparkSession factory with Delta Lake extensions; YAML config loader |
| `pipeline/run_all.py` | Entry point | Orchestrates Bronze → Silver → Gold; exits 0/1 |

---

## Design Decisions

### Surrogate Keys
Surrogate keys use `sha2(natural_key, 256)` → first 15 hex chars → `conv(hex, 16, 10).cast(BIGINT)`:
- **No global sort required** — eliminates `WindowExec` performance degradation
- **Deterministic** — same natural key always produces the same SK, enabling consistent FK joins
- **Collision-safe** — 2⁶⁰ key space; collision probability negligible for expected data volumes

### Delta JAR Bundling
Delta Lake JARs (`delta-spark`, `delta-storage`, `antlr4-runtime`) are copied into `/app/jars/` during the Docker build and referenced via `spark.jars` in `spark_utils.py`. No Maven/Ivy download occurs at runtime, making the build fully reproducible in an offline scoring environment.

### DQ Flags
Priority order (one flag per record): `NULL_REQUIRED` → `ORPHANED_ACCOUNT` → `DUPLICATE_DEDUPED` → `DATE_FORMAT` → `CURRENCY_VARIANT` → `TYPE_MISMATCH`. Clean records get `dq_flag = null`.

### Resource Constraints
- `local[2]`, `driver.memory=512m`, `executor.memory=1g`, `shuffle.partitions=4`

---

## Local Build & Test

### Prerequisites
- Docker Desktop running
- Official base image available: `nedbank-de-challenge/base:1.0`

### Build
```bash
docker build -t candidate-submission:latest .
```

### Run (Stage 1 data)
```bash
docker run --rm \
  -v /path/to/test_data:/data \
  -m 2g --cpus="2" \
  candidate-submission:latest
```

### Validate
```bash
docker run --rm \
  -v /path/to/test_data:/data \
  -m 2g --cpus="2" \
  candidate-submission:latest python /data/validate.py
```

Expected output: `ALL 3 VALIDATION QUERIES PASSED`

---

## Configuration

`config/pipeline_config.yaml` externalises all paths, Spark settings, and DQ thresholds.  
At runtime, the scoring system mounts its own config at `/data/config/pipeline_config.yaml` — the container reads from that path via the `PIPELINE_CONFIG` environment variable (default: `/data/config/pipeline_config.yaml`).

---

## AI Disclosure

This pipeline was developed with GitHub Copilot assistance. All code has been reviewed, understood, and can be explained by the author.
