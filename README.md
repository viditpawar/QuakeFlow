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

## Repository layout

The layout grows as features are built. See [decisions.md](decisions.md) for the reasoning behind project choices.
