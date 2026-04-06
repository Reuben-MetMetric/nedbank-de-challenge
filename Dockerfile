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

# Copy pipeline code, config and bundled Delta JARs.
# Data files are injected at runtime by the scoring system — do NOT copy them.
COPY pipeline/ pipeline/
COPY config/   config/
COPY jars/     jars/

# Run the complete pipeline end-to-end.
# No TTY or stdin is required.
CMD ["python", "pipeline/run_all.py"]
