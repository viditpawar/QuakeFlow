# The USGS collector as a container. Base image pinned by version and digest, and every Python package
# pinned and verified against the hashes in collector-requirements.txt (generated from the .in file).
FROM python:3.15.0-slim-bookworm@sha256:0e019bbdfb5d9874cb2389f1c2884b64aa2ac97c99960b5bc521cef978c0617d

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

RUN --mount=type=bind,source=docker/collector-requirements.txt,target=/tmp/requirements.txt \
    pip install --no-cache-dir --require-hashes --root-user-action=ignore -r /tmp/requirements.txt

# Runs as an unprivileged user. /data is the volume holding the collector's SQLite state and /outbox the
# volume shared with Airflow for reconciliation batches, and /history holds the files of a backfill. All are
# created here and owned by that user so a new named volume mounted on them is writable.
RUN useradd --uid 10001 --no-create-home --shell /usr/sbin/nologin quakeflow \
    && mkdir -p /data /outbox /history \
    && chown 10001:10001 /data /outbox /history

WORKDIR /app
COPY ingestion ./ingestion
COPY schemas ./schemas
COPY processing ./processing
# Only the backfill, which the backfill DAG runs here. The Spark jobs in batch/ live in the Spark image.
COPY batch/__init__.py ./batch/__init__.py
COPY batch/backfill.py ./batch/backfill.py
COPY configs/sources.yaml ./configs/sources.yaml

# Inside the Compose network Kafka is reached by its service name, never by 127.0.0.1, which here
# would mean this container itself.
ENV KAFKA_BOOTSTRAP_SERVERS=kafka:19092 \
    QUAKEFLOW_SOURCES_CONFIG=/app/configs/sources.yaml \
    QUAKEFLOW_STATE_DB=/data/quakeflow_state.db \
    QUAKEFLOW_OUTBOX_DIR=/outbox

USER 10001
HEALTHCHECK --interval=30s --timeout=5s --start-period=30s --retries=3 \
    CMD ["python", "-m", "ingestion.healthcheck"]
CMD ["python", "-m", "ingestion.live.poller", "--metrics-port", "9108", "--metrics-address", "0.0.0.0"]
