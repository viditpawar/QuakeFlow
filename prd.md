# Product Requirements: QuakeFlow

What is being built, the problem it solves, and what it must do. Status as of 2026-10-16. For how it is built see [architecture.md](architecture.md), for why each choice was made see [decisions.md](decisions.md), for what is left see [tasks.md](tasks.md).

## 1. The problem

USGS does not publish an earthquake once and leave it. It keeps revising the record, sometimes for days: the magnitude changes, the location and depth move, the review status goes from *automatic* to *reviewed*. A pipeline that treats every report as a new earthquake double counts, and one that overwrites in place loses the history, so neither can answer "what did we believe at 10:05, and what do we believe now".

On one real day, 19 events were revised: 9 changed review status, 7 changed depth, 7 changed location, 7 changed event time and only 5 changed magnitude. A pipeline that watched magnitude alone would have missed most changes.

**The product is a data platform that gets revisions right.** It collects live and historical USGS data, keeps every distinct revision it observes, always exposes exactly one current version per event, and publishes analytics only when they pass written quality checks.

It is a data engineering project, not an earthquake prediction or warning system.

## 2. Who it is for

| Audience | What they need from it |
|---|---|
| The owner (single developer) | A project that shows real streaming and batch data engineering end to end: correctness under failure, orchestration, observability, honest measurement |
| A reviewer or hiring manager reading the repository | A clear README, evidence that each claim was tested, and an honest account of limits |
| Someone running it | One laptop, Docker, no paid services, no cloud account, no API key, commands that work in order |

## 3. Goals

1. **Correct revisions.** The same revision arriving many times, late, out of order, or from two paths is stored once, and the newest revision is current.
2. **Survive failures.** A killed worker, a Kafka outage, a lost topic, a killed Spark load and a cancelled job each have a tested recovery, with the data checked, not the logs.
3. **Trustworthy analytics.** Gold is published only when every quality check passes, and readers always see one complete build.
4. **Run itself.** Ingestion, reconciliation and analytics run on a schedule with retries and a run history, and a lost streaming job is brought back.
5. **Be observable.** Every stage exposes metrics, there is a dashboard and alert rules.
6. **Be honest.** Every guarantee is stated with its limit, and every measurement says what it does not show.
7. **Fit one laptop.** 16 GB of RAM, Docker Desktop, $0.

## 4. Non-goals

- Prediction, forecasting, or alerting people about earthquakes.
- High availability, multi-node scale, or backups. It survives process failures, not the loss of the machine.
- Real-time analytics at sub-second latency. Gold is up to about 30 minutes behind by design.
- Recovering revisions that were replaced before the platform saw them. USGS does not expose them.
- Production security. Airflow uses the development login manager and the scheduler holds the Docker socket, on a laptop bound to 127.0.0.1.
- A user-facing application. The interfaces are Grafana, the Airflow UI and the Flink UI.

## 5. Core features

| # | Feature | What it must do | Status |
|---|---|---|---|
| F1 | **Live collection** | Poll the USGS hour feed every 60 s and the day feed every 15 min, validate and fingerprint each record, send each revision to Kafka at least once, mark it sent only when Kafka confirms | Built, tested |
| F2 | **Revision identity** | `(event_id, record_hash)` per revision, the hash over source content only. One current row per event: newest `source_updated_at`, ties to the higher hash | Built, tested |
| F3 | **Stream validation** | Flink parses and validates every message, drops exact repeats, routes bad records to a rejected topic with the reason, writes transactionally, retries without limit | Built, tested |
| F4 | **Lakehouse** | Bronze (as received), Silver (every revision, current state, rejected records, a log of loads), Gold (analytics) in Iceberg, loaded by idempotent merges | Built, tested |
| F5 | **Historical backfill** | Download a date range from the catalog in windows, resume safely, verify the range is in Silver | Built, tested, runs as a DAG |
| F6 | **Reconciliation** | Find revisions the live feeds missed (updated after an event left the one-day feed) and hand them to the collector without a second writer, moving the watermark only on the collector's durable answer | Built, run live |
| F7 | **Quality gate and Gold** | Audit Silver and each result against a written contract, publish a versioned build in one commit only if every check passes, allow rollback to an earlier build | Built, tested |
| F8 | **Orchestration** | Airflow runs the pipeline every 30 min, reconciliation every 6 h, backfills on demand, with retries, timeouts, run history and one Spark container at a time | Built, run live |
| F9 | **Streaming job recovery** | Detect a cancelled or lost Flink job and submit it again from its checkpoint, never start from the beginning without a person | Built, run live |
| F10 | **Observability** | Metrics from the collector, Kafka, Flink and Spark jobs, a dashboard, 11 alert rules checked by tests | Built. Alerts are not delivered anywhere |
| F11 | **Recovery from lost Kafka data** | Resend revisions the collector had confirmed but Kafka lost, from the collector's own state | Built, used once for real |
| F12 | **Continuous verification** | Unit tests, integration and recovery tests against the real stack, CI on every push, a configuration validator | Built |
| F13 | **Performance measurement** | A synthetic-load suite with honest ranges and stated limits | Built, results on hold ([tasks.md](tasks.md)) |
| F14 | **Documentation** | README, architecture, data dictionary, runbook, verification log, decisions | Built, README refresh pending |

## 6. Acceptance criteria

The platform is acceptable when every row of the "Proven by tests" table in the README holds on a fresh run, and:

- After `docker compose` starts the stack and `scripts/airflow_setup.py` has run, the DAGs run without manual commands, and Gold is built from the Silver snapshots recorded by the load before it.
- Kill or cancel any one streaming component and, within its documented time, the data in the lakehouse is complete with each revision stored once. Verification counts distinct `(event_id, record_hash)`, not log lines.
- A failing quality check publishes nothing and the previous Gold build stays visible.
- Replaying, repeating or overlapping any load, backfill or reconciliation changes no row count.
- Every number in the documentation comes from a recorded run, and every evidence entry says what it does not show.

## 7. Data

| Source | Use | Notes |
|---|---|---|
| USGS earthquake GeoJSON feeds (`all_hour`, `all_day`) | Live collection | Public, no API key, refreshed about once a minute |
| USGS FDSN Event Web Service | Backfill and reconciliation | Public, 20,000 events per query, returns each event's current revision only |
| Synthetic fixtures and benchmark data | Tests and benchmarks | Never claimed to represent USGS |

No data files are committed. USGS is credited, and the project is not affiliated with it.

## 8. Constraints

- One Windows laptop with 16 GB of RAM, Docker Desktop with about 7.6 GB for its virtual machine, PowerShell as the shell. All services bound to 127.0.0.1.
- Python 3.11 on the host and in the collector. Spark's image runs Python 3.10, so code that runs there avoids 3.11-only features.
- Cost of zero: open-source software only, pinned versions.
- The owner makes every commit and push, and works one step at a time.

## 9. Risks and how they are handled

| Risk | Handling |
|---|---|
| Memory: the full stack plus a Spark job leaves little free RAM | Compose profiles, one Spark container at a time, container memory limits, memory measured and recorded |
| Silent data loss | Idempotent merges, counts verified from data, `failOnDataLoss` gap stated, arrival-time retention (D-032) |
| Overclaiming | Guarantees are written per hop, evidence entries state what they do not show, numbers come from recorded runs |
| Revisions the catalog cannot show | Stated as a limit. The reconciliation job narrows it, nothing closes it |
| Docker socket in Airflow | Mounted only into the scheduler, UI on 127.0.0.1, trust assumptions written down (D-027) |
| Drift between docs and code | A configuration validator and guard tests check the facts that docs rely on |

## 10. Success measures

| Measure | Result so far |
|---|---|
| Unit tests | 480 pass |
| Integration and recovery tests | 21, run locally against the real stack |
| Live reconciliation | 1,101 events returned, 684 new revisions accepted and delivered, then 0 new on the overlapping rerun |
| Real recovery | 23 of 23 lost revisions recovered. A cancelled Flink job resumed from its checkpoint |
| Throughput | Measured on synthetic data, rerun in progress ([docs/benchmarks.md](docs/benchmarks.md)) |
