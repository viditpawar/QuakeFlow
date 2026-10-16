# Tasks

What is done, what is in progress, what is next, and what is deferred. Update this file at the end of every step. For context see [decisions.md](decisions.md) section 1, and for the rules see [.rules.md](.rules.md). Status as of 2026-10-16.

## Now: resume here

The benchmark work is **on hold**. Do these in order when told to continue.

1. **See whether the rerun finished.** It was started in the background as two invocations: `--sizes 2000 10000 40000 --repeats 3`, then `--sizes 200000 --repeats 1`. Results are in `benchmarks/results/*.json` (the three files in `before-fix/` are the first, pre-fix run and are kept as evidence). A file with a `finished` field is complete. If nothing is running and a run is missing, rerun the missing sizes.
2. **Unpause the DAGs.** They were paused so the benchmark had the machine to itself:
   ```powershell
   foreach ($d in "incremental_pipeline","reconciliation","flink_health") { docker exec quakeflow-airflow-scheduler airflow dags unpause $d }
   ```
3. **Investigate the failed runs.** In the rerun, two runs failed (40,000 repeat 2 and 2,000 repeat 3, out of the 7 seen when the work was put on hold). The first: the Spark `ingest_live` container loaded its batch and then exited non-zero, and the cause was not captured. Spark's peak memory was 1,752 MiB of its 2,048 MiB limit, so it does not look like a memory kill, but that is not established. The harness now writes a failed container's full output and exit code to `benchmarks/results/failures/`. Run 40,000 a few more times (`python -m benchmarks.run_benchmark --sizes 40000 --repeats 3`) and read that file. Do not publish numbers until this is explained or clearly reported as an intermittent failure with its rate.
4. **Check the 200,000 run.** It exists to confirm that the retention loss (D-032) is gone: it deliberately uses 2025 update times, which lost data before the fix. If it passes, that is the regression evidence. If it fails, read `failures/` before anything else.
5. **Put the real numbers into the docs.** `docs/benchmarks.md` currently holds the pre-fix table and prose, and must be rewritten from the new result files (`python -m benchmarks.run_benchmark --summarize <file>`). Keep the caveats section, add the 200,000 result and any failure rate, and correct the run time (the first full run took 28 minutes, the document says 38).
6. **Apply the README refresh.** A full draft is in `data/drafts/README.next.md` (git-ignored, delete it afterwards). It has two placeholders: `{{BENCHMARK_SUMMARY}}`, to be replaced with the compact table (`--summarize` prints it), and `{{UNIT_COUNT}}`. Check the "Performance" paragraph against the final numbers. The current `README.md` is the old version, written before Airflow.
7. **Commit.** Uncommitted: the D-032 fix and its tests, `benchmarks/`, `docs/benchmarks.md`, the new top-level documents, the `decisions.md` restructure.

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

Evidence for each is in `docs/verification-log.md`, with what it does not show.

## Remaining from the roadmap

| # | Step | Owner | State |
|---|---|---|---|
| 4 | Benchmark the pipeline: synthetic load, honest ranges, no fabricated numbers | assistant | Suite built and run. Rerun after the D-032 fix is in progress, one run failed unexplained. Docs hold pre-fix numbers. See "Now" |
| 5 | Demo polish: README refresh and benchmark summary | assistant | README draft written, waiting for the final numbers |
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
