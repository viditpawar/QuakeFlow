# StreamLake

Real-time and batch data engineering platform for NYC Citi Bike data, built with Apache Kafka, Flink, PySpark, and Apache Iceberg, and run locally with Docker.

> **Status:** early development. Nothing here is a measured result yet.

## What it does

- Polls live station availability from the public Citi Bike GBFS feed and publishes it to Kafka.
- Processes the stream with Flink (validation, deduplication, event-time metrics).
- Ingests historical monthly trip files with PySpark.
- Stores data in Bronze / Silver / Gold Apache Iceberg tables.
- Adds data quality gates, monitoring, and failure-recovery tests.

## Data sources and notice

- Live: https://gbfs.citibikenyc.com/gbfs/gbfs.json
- Historical: official Citi Bike trip history archive.

This repository contains code, schemas, docs, tests, and small synthetic fixtures only. It does not redistribute Citi Bike data. Download real data from the official provider yourself, and follow its data-use policy. This project is not affiliated with Citi Bike or Lyft.

## Repository layout

The layout grows as features are built. See [decisions.md](decisions.md) for the reasoning behind project choices.
