FROM nedbank-de-challenge/base:1.0

# Install any additional Python packages beyond the base image.
# The base image already includes:
#   pyspark==3.5.0, delta-spark==3.1.0, pandas==2.1.0,
#   pyarrow==14.0.0, pyyaml==6.0.1, duckdb==0.10.0
WORKDIR /app

# Ensure /app is in the Python module search path so that
# `from pipeline.xxx import ...` works when invoked as
# `python pipeline/run_all.py` (which sets sys.path[0] to /app/pipeline).
ENV PYTHONPATH=/app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy pipeline code and config.
# Data files are injected at runtime by the scoring system — do NOT copy them.
COPY pipeline/ pipeline/
COPY config/   config/

# Pre-warm the Delta Lake Ivy resolution cache during image build.
# The scoring container has NO internet access at runtime — Spark must find
# the Delta JARs already cached in /root/.ivy2/ without downloading them.
# This RUN step executes with internet access (build time only).
RUN python -c "\
from delta import configure_spark_with_delta_pip; \
from pyspark.sql import SparkSession; \
b = SparkSession.builder.master('local[1]').appName('prewarm') \
    .config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension') \
    .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog'); \
spark = configure_spark_with_delta_pip(b).getOrCreate(); \
spark.stop(); \
print('Delta JAR cache warm.')"

# Run the complete pipeline end-to-end.
# No TTY or stdin is required.
CMD ["python", "pipeline/run_all.py"]
