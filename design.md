# Design

The contracts and conventions that keep the system consistent: the data model, the message and table contracts, the patterns every stage reuses, naming, and how the interfaces (Grafana, Airflow, Flink) are laid out. [architecture.md](architecture.md) shows what is connected to what, and [decisions.md](decisions.md) says why. Every table and column is in [docs/data-dictionary.md](docs/data-dictionary.md).

## 1. Design principles

1. **A revision is the unit, not an event.** Identity is `(event_id, record_hash)`. Everything else follows: merges, deduplication, replay and reconciliation all use the same key.
2. **Idempotent by construction.** Every write is a MERGE on the target's identity, so running anything twice, late, or overlapping changes nothing. Correctness never depends on a checkpoint, an offset or a schedule being right.
3. **Nothing is dropped silently.** Invalid input is kept with the reason (the rejected topic and `silver.rejected_records`), late and conflicting revisions are kept and flagged. Where something is skipped without failing (a deleted Kafka range), it is written down as a known gap.
4. **Acknowledge only what is durable.** A revision is "sent" when Kafka confirms. A reconciliation batch is "accepted" when the collector has committed it. A Gold build is "published" in one commit. Each state means one thing and is tracked on its own.
5. **Audit, then publish.** Write somewhere readers cannot see, check, then switch in one commit.
6. **One writer per piece of state.** The collector alone writes its SQLite file. Airflow reaches it through files, never through its database.
7. **Say what a guarantee does not cover.** Every guarantee is stated per hop, and every piece of evidence says what it does not show.
8. **Fit the machine.** Profiles, memory limits, one Spark container at a time.

## 2. The data model

### The event

One normalized earthquake revision, as sent through Kafka and stored in Silver (`schemas/earthquake.py`, frozen Pydantic model):

| Field | Type | Notes |
|---|---|---|
| `event_id` | string | USGS id, the Kafka key |
| `event_time` | UTC timestamp | when the earthquake happened |
| `source_updated_at` | UTC timestamp | when USGS last changed the record, the ordering key |
| `magnitude` | float or null | USGS does not always assign one |
| `latitude`, `longitude` | float | within -90..90 and -180..180 |
| `depth_km` | float or null | can be negative |
| `place`, `review_status`, `event_type` | string or null | `event_type` is not only `earthquake` |
| `ingested_at` | UTC timestamp | added by the platform, not part of the hash |
| `record_hash` | 64 hex characters | SHA-256 of the canonical source content |

### Identity and ordering

- **Hash:** SHA-256 over the ten source fields in `HASHED_FIELDS`, as canonical JSON (sorted keys, no spaces, timestamps as epoch milliseconds, `-0.0` as `0.0`) plus `hash_version`. `ingested_at` is excluded. Changing the field list or encoding changes every hash and needs a `HASH_VERSION` bump and a decision entry.
- **Current state:** per event, the revision with the newest `source_updated_at`. On an exact tie the higher `record_hash`. An older revision never replaces a newer one.
- **Change kinds:** `new`, `revised`, `out_of_order` (older than a known revision), `conflict` (same update time, different content).

### Layers

| Layer | Tables | Identity |
|---|---|---|
| Bronze | `live_observations`, `historical_observations` | live: Kafka topic, partition, offset and message text. historical: `(event_id, payload_sha256)` |
| Silver | `earthquake_versions` | `(event_id, record_hash)`, insert only |
| | `earthquakes_current` | `event_id` |
| | `rejected_records` | the Kafka coordinates plus the payload |
| | `load_log` | one row per finished load, the Silver snapshots it ended with |
| Gold | `<name>_builds` for five analytics tables | `build_id` plus the table's own key |
| | `build_manifest`, `published_build`, `build_log` | the build's id |
| | five views named after the analytics | show the published build only |

Gold is earthquakes only (`event_type = 'earthquake'`), with days and months in UTC. Silver and Bronze keep every event type.

## 3. Contracts

### Kafka

| Topic | Key | Value | Partitions | Retention |
|---|---|---|---|---|
| `quakeflow.events.raw` | `event_id` | the normalized event as JSON | 3 | 7 days from arrival |
| `quakeflow.events.validated` | `event_id` | the same text, unchanged | 3 | 7 days from arrival |
| `quakeflow.events.dlq` | none | JSON with `reason`, `source_topic`, `source_partition`, `source_offset`, `payload` | 1 | 30 days from arrival |

- Headers on raw: `source`, `schema_version`, `change_kind`, `record_hash`.
- All topics use `message.timestamp.type=LogAppendTime` (D-032). The producer's timestamp, the USGS update time, is replaced by arrival time. A validator check enforces it.
- Flink writes in transactions, so every reader uses `read_committed`.
- Topics are defined in `configs/kafka-topics.yaml` and created, and kept in line, by `infrastructure/kafka/create_topics.py`.

### The reconciliation outbox (D-029)

Volume `reconcile-outbox`, shared by the collector and the containers Airflow starts.

```
tmp/       files being written, never read
ready/     <batch_id>.json  complete batches, moved in with one atomic rename
status/    <batch_id>.json  the collector's answer: accepted, published or rejected
rejected/  batches that failed validation
```

A batch file holds `format`, `batch_id`, `created_at`, `source` (query boundaries and `queried_at`), `count`, `sha256` of the canonical features, and the features. The collector validates it, commits the batch and its new revisions in one SQLite transaction, then writes the status file, then removes the ready file. Airflow moves its watermark (an Airflow Variable) to the batch's `queried_at` only after it reads `accepted` or `published`, and never backwards. A batch ID seen again with other content is rejected.

### Gold publication (D-028)

1. A load records its Silver snapshot IDs in `silver.load_log` under the Airflow run ID.
2. A Gold build reads Silver at those snapshots (`<table>.snapshot_id_<id>`), stages five results as temporary views, and runs the checks in `configs/quality.yaml` plus column and reconciliation checks.
3. If all pass it appends each result to `<name>_builds` under a new `build_id`, confirms the row counts, records the build in `build_manifest`, and replaces the one row of `published_build` in a single commit.
4. The five views filter on that row. An earlier build can be published again with `--publish-existing`. The newest 10 builds are kept.

### The quality contract

`configs/quality.yaml` lists datasets with checks (`not_null`, `unique`, `between`, `min_rows`) and reconciliations (a staged result must account for exactly the events in Silver). Anything that fails or cannot run blocks publication.

## 4. Conventions

### Naming

| Thing | Convention |
|---|---|
| Kafka topics | `quakeflow.events.<stage>`, throwaway test topics `quakeflow.test.<suffix>.<stage>` |
| Iceberg catalog and namespaces | catalog `local`, namespaces `local.bronze`, `local.silver`, `local.gold`. Throwaway namespaces `local.zt<suffix>_<layer>` (tests) and `local.zb<suffix>_<layer>` (benchmarks) |
| Containers | `quakeflow-<service>` |
| Volumes | Compose names, prefixed `quakeflow_` on the host |
| Decisions | `D-nnn`, referenced from code comments and docs |
| Build ids | UUIDs, validated by a pattern before they reach SQL |
| Batch ids | the Airflow run ID with unsafe characters replaced |

### Code

- Python 3.11, ruff with line length 100. `batch/` runs in the Spark image on Python 3.10, so it avoids 3.11-only features such as `datetime.UTC`. A test enforces it and `pyproject.toml` lists the exceptions.
- Files that Airflow imports and that run in its image use the standard library only.
- Docstrings say what a module guarantees and what it does not. Comments explain a reason, not the line.
- Host-side addresses are `127.0.0.1`, never `localhost`. Container-side addresses are service names, never `127.0.0.1`.
- Anything pinned stays pinned: images by version (the collector's base by digest), Python packages by hash in `docker/collector-requirements.txt` and `docker/spark-requirements.txt`, Actions by commit SHA.
- SQL files are split on semicolons, so no semicolons inside comments or strings.

### Configuration

Configuration is files: `sources.yaml`, `kafka-topics.yaml`, `quality.yaml`, `prometheus.yml`, `alerts.yml`. `scripts/validate_config.py` checks them, and checks that compose, `.env.example`, ports, the Kafka data directory and the topic timestamp setting agree. Secrets are never committed: `.env` and `secrets/` are git-ignored, and the validator fails on a committed key.

## 5. Observability design

- **Naming:** `quakeflow_<thing>` with `_total` for counters and `_seconds` for durations. Labels are small fixed sets (feed, kind of change, outcome, job). An event id or other per-record value is never a label, and a test checks it.
- **Counters start at zero** (`initialise`) so a dashboard shows 0 and not "no data".
- **Scraped:** the collector (`:9108`), Flink (`:9249`), Kafka through kafka-exporter. **Pushed:** Spark jobs, which end before a scrape, push duration, success and last-success time to the Pushgateway, and a failed push never fails the job.
- **Alerts:** 11 rules in `monitoring/alerts.yml`, tested with `promtool` (`alerts_test.yml`). Delivery is not set up.
- **Dashboard** `quakeflow-operations`, one page, top to bottom: a row of current numbers (events ingested, new revisions, source freshness, pending revisions, invalid records, consumer lag), then Collector (rates by feed and by kind, freshness, ingestion latency, invalid records, poll outcomes), then Kafka and Flink (consumer lag, checkpoints and their duration), then Lakehouse (Spark job duration and time since last success), then the Gold quality gate.

## 6. Orchestration design

- **One DAG per concern,** all with `max_active_runs=1`, `catchup=False`, and created paused.
- **Spark tasks** use `DockerOperator` with the existing images, `mount_tmp_dir=False`, credentials in `private_environment`, and the pool `quakeflow_spark` (one slot).
- **Idempotent tasks.** A retried fetch finds its own batch, a retried ingest merges again, a resubmit does nothing if the job is already running.
- **Decision logic lives in plain functions** (`preflight`, `watermark`, `backfill_params`, `flink_health`) that are unit-tested, and the DAG file only wires them.
- **Do not pass an argument ending in `.sh` to a templated field.** Airflow reads it as a template file. Run scripts through `sh -c`.
- **Refuse to guess.** Where a wrong automatic action would emit or lose data (no checkpoint, unreachable JobManager, an unknown watermark), the run fails and a person decides.

## 7. Test design

| Level | What it proves | Where |
|---|---|---|
| Unit | Logic, guards and configuration, with fakes and temporary directories. No Docker | `tests/unit` |
| Integration | One component against the real stack on throwaway topics and namespaces, cleaned up afterwards | `tests/integration` |
| Recovery | A failure is caused (a kill, a cancel, a lost topic) and the data is counted | `tests/recovery` |
| Benchmark | Throughput and memory under synthetic load, with every run's counts checked first | `benchmarks/` |

Rules the tests follow: verify from data, not logs. Write the failing test first when a bug is found. Run a **negative control** (the same scenario against the broken or unprotected variant) for any guard, so a test that cannot fail is not trusted. Throwaway resources are removed even on failure.
