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

Run the tests and linter with `python -m pytest` and `python -m ruff check .`. Stop Kafka with `docker compose --profile core down`.

## Repository layout

The layout grows as features are built. See [decisions.md](decisions.md) for the reasoning behind project choices.
