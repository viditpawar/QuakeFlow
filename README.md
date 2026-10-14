# QuakeFlow

Real-time and batch data engineering platform for USGS earthquake events, built with Apache Kafka, Flink, PySpark, and Apache Iceberg, and run locally with Docker.

> **Status:** early development. Nothing here is a measured result yet.

QuakeFlow is a data infrastructure project. It is not an earthquake prediction or warning system.

## What it does

- Polls the live USGS earthquake GeoJSON feed and publishes new and revised events to Kafka.
- Processes the stream with Flink (validation, deduplication, revision detection).
- Backfills historical events from the USGS catalog API with PySpark.
- Stores data in Bronze / Silver / Gold Apache Iceberg tables, keeping every observed revision of an event plus one current version.
- Adds data quality gates, monitoring, and failure-recovery tests.

## Data sources and notice

- Live: USGS earthquake GeoJSON feeds (https://earthquake.usgs.gov/earthquakes/feed/)
- Historical: USGS FDSN Event Web Service (https://earthquake.usgs.gov/fdsnws/event/1/)

USGS data is public, and no API key is needed. Credit USGS as the source. This repository contains code, schemas, docs, tests, and small synthetic fixtures only, and this project is not affiliated with or endorsed by USGS.

## Running locally

Windows PowerShell, Python 3.11, Docker Desktop. Run from the repository root.

```powershell
py -3.11 -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -e ".[dev]"

docker compose --profile core up -d          # Kafka (KRaft, single broker)
python infrastructure\kafka\create_topics.py # first time only; safe to repeat
python -m ingestion.live.poller              # poll both USGS feeds and publish to Kafka
```

Useful flags for the poller: `--once` polls every feed a single time and exits (non-zero if Kafka did not confirm everything), and `--no-publish` only detects changes without contacting Kafka. Feed URLs and intervals are in `configs/sources.yaml`. Collector state is kept in a local SQLite file (`data/quakeflow_state.db` by default), which is not committed.

Stream processing (Flink 2.2.1, adds about 1.5 GB of RAM) runs under a separate profile, together with Kafka:

```powershell
docker compose --profile core --profile streaming up -d --build
docker exec quakeflow-flink-jobmanager ./bin/sql-client.sh -i /opt/flink/sql/kafka_sources.sql -f /opt/flink/sql/smoke_count.sql
```

Start the validation and deduplication job (reads `quakeflow.events.raw`, writes `quakeflow.events.validated` and `quakeflow.events.dlq`):

```powershell
docker exec quakeflow-flink-jobmanager sh -c "cat /opt/flink/sql/kafka_sources.sql /opt/flink/sql/validated_events.sql /opt/flink/sql/revision_tracking.sql > /tmp/job.sql && ./bin/sql-client.sh -f /tmp/job.sql"
```

The sinks are exactly-once, so anything reading the output topics must use `isolation.level=read_committed`. The Flink web UI is at http://localhost:8081. Cancel the job there, or with `curl -X PATCH "localhost:8081/jobs/<job id>?mode=cancel"` inside the jobmanager container.

The integration and recovery tests run the real job on throwaway topics (they need the stack running): `python -m pytest -m integration tests/integration tests/recovery`. See [docs/recovery-runbook.md](docs/recovery-runbook.md) for failure recovery.

Lakehouse (PostgreSQL catalog, RustFS object storage, Spark 4.0 and Iceberg), separate profile:

```powershell
docker compose --profile batch up -d postgres rustfs
python infrastructure\storage\create_bucket.py   # first time only; safe to repeat
docker compose --profile batch run --rm spark /opt/spark/bin/spark-sql -f /workspace/iceberg/sql/001_create_namespaces.sql
docker compose --profile batch run --rm spark /opt/spark/bin/spark-sql -e "SHOW NAMESPACES"
```

Spark runs as a one-off container per command, so it uses memory only while a job runs. The RustFS console is at http://localhost:9001. Default credentials are local-only and can be changed in `.env` (see `.env.example`).

Load Kafka into the lakehouse (Bronze, Silver versions, Silver current state, rejected records). Each run reads what is new and exits, so run it again whenever you want the lakehouse brought up to date. Kafka and the lakehouse must be running:

```powershell
docker compose --profile core --profile batch run --rm spark /opt/spark/bin/spark-submit /workspace/batch/ingest_live.py
docker compose --profile batch run --rm spark /opt/spark/bin/spark-sql -e "SELECT event_id, magnitude, place, event_time FROM local.silver.earthquakes_current ORDER BY event_time DESC LIMIT 20"
```

Running the job twice, or after deleting its checkpoint, leaves the tables unchanged. Integration tests for it: `python -m pytest -m integration tests/integration/test_ingest_live.py` (about 6 minutes).

Run the tests and linter with `python -m pytest` and `python -m ruff check .`. Stop Kafka with `docker compose --profile core down`.

## Repository layout

The layout grows as features are built. See [decisions.md](decisions.md) for the reasoning behind project choices.
