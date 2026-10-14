# Monitoring

Prometheus collects the numbers, Grafana shows them, and a set of alert rules says when something looks wrong.

```powershell
docker compose --profile core --profile streaming --profile batch --profile monitoring up -d
python -m ingestion.live.poller        # serves collector metrics on http://127.0.0.1:9108/metrics
$env:PUSHGATEWAY_URL = "http://pushgateway:9091"   # lets the Spark jobs report to Prometheus
```

| What | Where |
|---|---|
| Grafana dashboard "QuakeFlow operations" | http://localhost:3000 (user `admin`, password `GRAFANA_ADMIN_PASSWORD`, default `quakeflow-local`) |
| Prometheus, targets and alert rules | http://localhost:9090 (`/targets`, `/alerts`) |
| Pushgateway | http://localhost:9091 |
| Flink web UI | http://localhost:8081 |

Everything binds to localhost. Measured with the whole stack running, the four monitoring containers used about 230 MB together (Grafana 119 MB, Prometheus 67 MB, Pushgateway 23 MB, kafka-exporter 19 MB). Prometheus will grow as it keeps more history, up to its 1 GB or 15 day limit.

## Where each number comes from

| Source | How it is collected |
|---|---|
| Collector | The collector process serves `/metrics` and Prometheus scrapes it. The collector runs on the host, so Prometheus reaches it through `host.docker.internal`. |
| Flink | Flink's built-in Prometheus reporter on port 9249 of the JobManager and TaskManager. |
| Kafka consumer lag | `kafka-exporter`, which reports lag per consumer group, topic and partition. |
| Spark jobs | The jobs are one-off containers, so each pushes its result to the Pushgateway when it finishes. Pushing is off unless `PUSHGATEWAY_URL` is set, and a failed push never fails the job. |

## Metrics

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `quakeflow_events_ingested_total` | counter | `feed` | Valid events read from a feed, before deduplication. |
| `quakeflow_revisions_detected_total` | counter | `kind` | Distinct revisions seen for the first time. Kinds: `new`, `revised`, `out_of_order`, `conflict`. |
| `quakeflow_revisions_published_total` | counter | | Revisions that Kafka confirmed. |
| `quakeflow_invalid_records_total` | counter | `feed` | Feed records that failed validation. |
| `quakeflow_feed_polls_total` | counter | `feed`, `outcome` | Polls by `ok` or `error`. |
| `quakeflow_last_successful_poll_timestamp_seconds` | gauge | `feed` | When the feed last polled successfully. |
| `quakeflow_pending_revisions` | gauge | | Detected but not yet confirmed by Kafka. |
| `quakeflow_source_freshness_seconds` | gauge | | Age of the newest USGS update seen, worked out when scraped, so it keeps growing between polls. |
| `quakeflow_ingestion_latency_seconds` | histogram | | From a revision's USGS update to Kafka confirming it. |
| `kafka_consumergroup_lag` | gauge | group, topic, partition | Records a consumer group has not read yet. |
| `flink_jobmanager_job_numberOfFailedCheckpoints` and related | gauge | job | Flink checkpoint health. |
| `quakeflow_spark_job_duration_seconds`, `_success`, `_last_run_timestamp_seconds`, `_last_success_timestamp_seconds` | gauge | `job` | The last run of each Spark job. |
| `quakeflow_gold_build_blocked`, `quakeflow_gold_checks_failed` | gauge | `job` | Whether the quality gate blocked the last Gold build, and how many checks failed. |

Labels are small fixed sets: feed names, kinds of change and outcomes. Event ids and other per-record values are never labels, because each distinct value would create a new time series. A unit test checks this.

Counters with labels start at zero when the collector starts, so a dashboard shows 0 and not "no data" before the first event.

## Alerts

`monitoring/alerts.yml` defines 11 rules, tested with `promtool` against made-up metric histories (`monitoring/alerts_test.yml`). No Alertmanager is run, so alerts are shown at `/alerts` in Prometheus and are not sent anywhere.

| Alert | Fires when |
|---|---|
| `CollectorDown` | The collector's metrics cannot be scraped for 3 minutes. |
| `SourceDataStale` | The newest USGS update seen is over 30 minutes old, for 5 minutes. |
| `FeedPollsFailing` | A feed failed at least 3 polls in 15 minutes. |
| `RevisionsNotReachingKafka` | Revisions have been pending for over 10 minutes. |
| `InvalidRecordsAppearing` | More than 10 records failed validation in an hour. |
| `KafkaConsumerLagHigh` | A consumer group is over 1000 records behind for 10 minutes. |
| `FlinkCheckpointsFailing` | Any Flink checkpoint failed in the last 10 minutes. |
| `FlinkNoRunningJob` | The Flink cluster has no running job for 5 minutes. |
| `SparkJobFailed` | The last run of a Spark job failed. |
| `SparkJobNotRunRecently` | The live load has not succeeded for over a day. |
| `GoldBuildBlocked` | The quality gate blocked the last Gold build. |

Run the rule tests with:

```powershell
docker run --rm --entrypoint promtool -v "${PWD}/monitoring:/work:ro" prom/prometheus:v3.15.0 test rules /work/alerts_test.yml
```

## Limits

- Thresholds are starting points for a local setup, chosen without a long baseline.
- Kafka lag is shown for consumer groups that commit offsets, which means the Flink job. The Spark load tracks its own offsets in a checkpoint, so its lag is not measured. How far behind it is shows up as how old the last successful run is.
- The collector must be running for its metrics to exist. If it is not, only `CollectorDown` tells you.
- The Spark job metrics describe the last run only. History comes from Prometheus, which keeps 15 days or 1 GB.
