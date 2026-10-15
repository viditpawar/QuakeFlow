# Running QuakeFlow

Every command for starting and operating the platform. These are PowerShell commands for Windows with Docker Desktop and Python 3.11, run from the repository root. For a one-page overview, see the [README](../README.md).

Compose profiles keep memory use manageable. Start only what you need:

| Profile | Services | Starts |
|---|---|---|
| `core` | Kafka | `docker compose --profile core up -d` |
| `streaming` | Flink JobManager and TaskManager | add `--profile streaming` |
| `batch` | PostgreSQL, RustFS, and Spark (run per command) | add `--profile batch` |
| `monitoring` | Prometheus, Grafana, Pushgateway, Kafka lag exporter | add `--profile monitoring` |

Copy `.env.example` to `.env` to change the local credentials. The defaults are for local use only, and every service binds to localhost.

## 1. Set up

```powershell
py -3.11 -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -e ".[dev]"
```

## 2. Collect live data

```powershell
docker compose --profile core up -d --build    # Kafka and the collector
python infrastructure\kafka\create_topics.py   # first time only, safe to repeat
docker logs -f quakeflow-collector             # watch it poll and publish
```

The collector runs as a container (`quakeflow-collector`) and restarts by itself. It polls the hour feed every 60 seconds and the day feed every 15 minutes (`configs/sources.yaml`). Its state, which revisions it has seen and sent, is a SQLite file on the volume `collector-state`, so it survives the container being removed. If the topics do not exist yet when it starts, its revisions simply wait and are delivered once you have created them.

**Run exactly one collector.** Its state has a single writer, and two collectors would each send every revision. For development you can run one on the host instead, with `python -m ingestion.live.poller` (flags: `--once` polls each feed a single time and exits, `--no-publish` only detects changes). It then uses `data/quakeflow_state.db` and a host Kafka address of `127.0.0.1:9092`. Stop the container first with `docker compose --profile core stop collector`. Note that the file `data/quakeflow_state.db` on the host is the state from before the collector moved into a container and is no longer current: the live state is on the volume `collector-state`. Starting a host collector from the old file would resend everything the container has sent since.

## 3. Validate and deduplicate with Flink

```powershell
docker compose --profile core --profile streaming up -d --build
```

Start the job, which reads `quakeflow.events.raw` and writes `quakeflow.events.validated` and `quakeflow.events.dlq`:

```powershell
docker exec quakeflow-flink-jobmanager sh -c "cat /opt/flink/sql/kafka_sources.sql /opt/flink/sql/validated_events.sql /opt/flink/sql/revision_tracking.sql > /tmp/job.sql && ./bin/sql-client.sh -f /tmp/job.sql"
```

The output topics are written with Kafka transactions, so anything reading them must use `isolation.level=read_committed`. The Flink web UI is at http://localhost:8081. To restart a job from a saved checkpoint, see the [recovery runbook](recovery-runbook.md).

## 4. Set up the lakehouse

```powershell
docker compose --profile batch up -d postgres rustfs
python infrastructure\storage\create_bucket.py   # first time only, safe to repeat
docker compose --profile batch run --rm spark /opt/spark/bin/spark-sql -f /workspace/iceberg/sql/001_create_namespaces.sql
docker compose --profile batch run --rm spark /opt/spark/bin/spark-sql -f /workspace/iceberg/sql/002_create_tables.sql
```

Spark runs as a one-off container per command, so it uses memory only while it runs. The RustFS console is at http://localhost:9001.

## 5. Load Kafka into the lakehouse

Each run reads what is new since the last one and exits, so run it whenever you want the lakehouse brought up to date. Kafka and the lakehouse must be running.

```powershell
docker compose --profile core --profile batch run --rm spark /opt/spark/bin/spark-submit /workspace/batch/ingest_live.py
docker compose --profile batch run --rm spark /opt/spark/bin/spark-sql -e "SELECT event_id, magnitude, place, event_time FROM local.silver.earthquakes_current ORDER BY event_time DESC LIMIT 20"
```

Running the job twice, or after deleting its checkpoint, leaves the tables unchanged.

## 6. Backfill historical data

The range is start inclusive, end exclusive, in UTC. Re-running the same command skips windows that are already complete, and windows over the service's 20,000-event limit are split automatically.

```powershell
python -m batch.backfill --start 2025-01-01 --end 2025-01-08 --min-magnitude 2.5
docker compose --profile batch run --rm spark /opt/spark/bin/spark-submit /workspace/batch/ingest_history.py
```

The first command writes a raw download, normalized files and a manifest for each window under `data/history/` (not committed). The second merges the completed windows into the lakehouse. Loading twice, or loading overlapping ranges, leaves the tables unchanged, and an older revision never replaces a newer current one.

## 7. Build the Gold analytics

The job audits Silver against `configs/quality.yaml` first and publishes only if every check passes. If any check fails, Gold is left untouched, the exit code is 1, and the attempt is recorded in `gold.build_log`.

```powershell
docker compose --profile batch run --rm spark /opt/spark/bin/spark-submit /workspace/batch/build_gold.py
docker compose --profile batch run --rm spark /opt/spark/bin/spark-sql -e "SELECT status, checks_failed, failed_checks FROM local.gold.build_log ORDER BY built_at DESC LIMIT 5"
```

The tables and their definitions are in the [data dictionary](data-dictionary.md).

## 8. Monitor

```powershell
docker compose --profile core --profile streaming --profile batch --profile monitoring up -d
$env:PUSHGATEWAY_URL = "http://pushgateway:9091"   # lets Spark jobs report their results
```

Grafana is at http://localhost:3000 (user `admin`, password `quakeflow-local` unless changed) and Prometheus at http://localhost:9090. See [monitoring](monitoring.md) for the metrics and alerts.

## Tests

```powershell
python -m pytest                          # unit tests
python -m ruff check .                    # lint
python scripts\validate_config.py         # configuration files, dashboards and contracts
python -m pytest -m integration           # needs the stack running, takes a long time
```

The integration and recovery tests run against the real services on throwaway topics and namespaces and clean up after themselves. Run a single file to keep it short, for example `python -m pytest -m integration tests/integration/test_build_gold.py` (about 10 minutes).

Alert rules are tested with Prometheus's own tool:

```powershell
docker run --rm --entrypoint promtool -v "${PWD}/monitoring:/work:ro" prom/prometheus:v3.15.0 test rules /work/alerts_test.yml
```

## Stopping

```powershell
docker compose --profile core --profile streaming --profile batch --profile monitoring down
```

This keeps the data. Add `-v` to also delete the volumes, which erases Kafka, the lakehouse, Flink checkpoints and Grafana.

## Notes

- The Spark image runs Python 3.10, so code in `batch/` that runs there (everything except `backfill.py`) must not use features that need 3.11. A unit test checks this.
- The Flink job and the Spark load do not start by themselves after a restart. Start them again with the commands above.
