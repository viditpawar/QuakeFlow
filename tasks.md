# Tasks

What is done, what is in progress, what is next, and what is deferred. Update this file at the end of every step. For context see [decisions.md](decisions.md) section 1, and for the rules see [.rules.md](.rules.md). Status as of 2026-10-16.

## Now: what is left

Everything the assistant had to build is built, tested and documented. What is left is the owner's:

1. **Commit and push** what is uncommitted (the commands were given with the last step).
2. **Screenshots and a short screen recording** for the demo: the Grafana operations dashboard with data in it, the Airflow DAG list with run history, a green `incremental_pipeline` graph, a `reconciliation` run, the Flink job RUNNING with checkpoints, a Spark ingest log line showing a count. A recording could show the Flink job being cancelled and `flink_health` bringing it back within 5 minutes.
3. **Delete the draft** `data/drafts/README.next.md` if it is still there (it is git-ignored and already applied to `README.md`).

Optional: a third benchmark repeat per size would tighten the ranges (`docs/benchmarks.md`, "What these runs do not show").

## Done

| Step | What | Where |
|---|---|---|
| Phase 1 | Collector: USGS clients, Pydantic model, record hash, SQLite change detector, Kafka publisher | `ingestion/`, `schemas/`, `processing/` |
| Phase 2 | Kafka and the publisher with delivery confirmation | `docker-compose.yml`, D-008, D-012 |
| Phase 3 | Flink validation and deduplication job | `flink/sql/` |
| Phase 4 | Iceberg lakehouse, Spark loads | `iceberg/`, `batch/ingest_live.py` |
| Phase 5 | Historical backfill and load | `batch/backfill.py`, `batch/ingest_history.py` |
| Phase 6 | Gold analytics and the quality gate | `batch/build_gold.py`, `configs/quality.yaml` |
| Phase 7 | Monitoring, dashboard, alert rules | `monitoring/` |
| Phase 8 | CI, configuration validation, documentation | `.github/`, `scripts/validate_config.py`, `docs/` |
| Review fixes | Flink retries without limit (D-023), Kafka on its volume and bound to localhost (D-024), Bronze identity (D-026), state-expiry and persistence tests | |
| Collector as a container | Hardened image, state on a volume, resend tool (D-025) | `docker/collector.Dockerfile`, `ingestion/resend.py` |
| Roadmap 1 | Airflow: incremental pipeline DAG, one-slot Spark pool (D-027, D-030) | `airflow/dags/incremental_pipeline.py` |
| Roadmap 3 | Versioned Gold, load log, rollback (D-028) | `batch/build_gold.py`, `batch/load_log.py` |
| Roadmap 2 | Reconciliation DAG with the outbox (D-029) | `airflow/dags/reconciliation.py`, `ingestion/outbox.py` |
| Roadmap 1 (cont.) | Backfill DAG and verification (D-030), Flink health DAG (D-031) | `airflow/dags/`, `batch/verify_history.py`, `flink/resubmit.sh` |
| Found by benchmark | Kafka retention counted from the USGS update time (D-032). Fixed, tested, applied to the live topics | `configs/kafka-topics.yaml`, `tests/integration/test_topic_timestamps.py` |
| Project documents | prd, architecture, design, tasks, rules, restructured decisions | repository root |
| Roadmap 4 and 5 | Benchmark suite and results, README rewritten for Airflow, versioned Gold and the benchmarks. Found and fixed the Spark memory limit (D-033) | `benchmarks/`, `docs/benchmarks.md`, `README.md` |

Evidence for each is in `docs/verification-log.md`, with what it does not show.

## Remaining from the roadmap

| # | Step | Owner | State |
|---|---|---|---|
| 4 | Benchmark the pipeline: synthetic load, honest ranges, no fabricated numbers | assistant | **Done.** Six runs plus a 200,000 check, all correct. The benchmarks found two defects, both fixed (D-032, D-033) |
| 5 | Demo polish: README refresh and benchmark summary | assistant | **Done** |
| 5 | Demo polish: screenshots of Grafana, Airflow and Flink, a short screen recording | **the owner** | Not started. Suggested shots below |

Suggested screenshots: the Grafana operations dashboard with data in it, the Airflow DAG list with run history, a green `incremental_pipeline` graph, the `reconciliation` run, the Flink job RUNNING with checkpoints, the Spark ingest log line showing a count. A recording could show the Flink job being cancelled and `flink_health` bringing it back.

## Backlog (not started, in rough priority order)

1. **Deliver alerts.** Add Alertmanager or another sink, and alert on a failed DAG run, a stalled outbox (`quakeflow_outbox_ready_batches` above 0 for long), a watermark that stops moving, and a failed `flink_health` run (D-021).
2. **Close the silent skip.** Decide whether the Spark load should fail on data loss instead of `failOnDataLoss=false`, or alert on Kafka's earliest offset moving past Spark's checkpoint (D-032).
3. **Iceberg maintenance.** Snapshot expiry and file compaction as a scheduled job. Neither exists, and `silver.load_log` pins snapshots, so expiry must keep the ones Gold may still read.
4. **Split large reconciliation windows.** A window over 20,000 updated events currently fails the run (D-029).
5. **Mark reconciled revisions** so `gold.ingestion_latency` does not count them as live (D-029).
6. **Restricted Docker socket proxy** for Airflow, and a real auth manager (D-027).
7. **Run the unrun failure paths live.** Stalled Flink job, unreachable JobManager, no checkpoint, rejected reconciliation batch, a failed backfill download, a backfill verification failure with a deleted revision as negative control.
8. **Backups** of the volumes, if the project is ever to hold data worth keeping.
9. **Prune the history volume** `quakeflow_history-files`, which grows with each backfill and is re-merged in full by every load.
10. **CI for Airflow:** import the DAG files in a container with Airflow installed, so a broken DAG fails on GitHub and not on the laptop.

## Working agreements

- One step at a time. Do not start the next step until the owner says so.
- The owner runs every `git commit` and `git push`. After each step, give the exact PowerShell commands, with a plain message and no AI attribution.
- Before a long job (more than a few minutes), say how long it will take.
- Verify counts from data. Report what was not run.
