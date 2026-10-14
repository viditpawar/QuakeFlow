# Recovery runbook

How to recover the streaming part of QuakeFlow, and what was measured. Timings are from one run on a
16 GB Windows laptop with Docker Desktop, single TaskManager, so treat them as examples, not guarantees.

## What survives what

| Failure | What happens | Data loss or duplicates |
|---|---|---|
| TaskManager container dies | The job restarts by itself from the latest checkpoint once a TaskManager is back. | None (tested) |
| Job cancelled or JobManager lost | Resubmit the job from the retained checkpoint (below). | None (tested) |
| Kafka stopped | The collector keeps unconfirmed revisions pending and sends them when Kafka returns. The Flink job keeps retrying, with a delay that grows to one minute, and resumes by itself. | None (both tested) |
| Spark load killed part way | Run the load again. Each table is a separate Iceberg commit, so tables can disagree with each other until the retry finishes (see below). | None (tested) |
| Checkpoint volume deleted | The job can only start fresh from `earliest-offset`. Output is reprocessed, so downstream must tolerate repeats. | Repeats possible |

There is no JobManager high availability. If the JobManager container dies, the running job is gone
until you resubmit it. Checkpoints are kept on cancel, so nothing is lost.

## TaskManager failure

```powershell
docker start quakeflow-flink-taskmanager      # if it is stopped
```

Nothing else is needed. Check that the job is back and was restored:

```powershell
docker exec quakeflow-flink-jobmanager curl -s localhost:8081/jobs/overview
docker exec quakeflow-flink-jobmanager curl -s localhost:8081/jobs/<job id>/checkpoints
```

`latest.restored` shows which checkpoint it resumed from. The job retries without a limit on the number of attempts: 10 seconds after the first failure, doubling up to one minute between tries. A fixed limit of 10 attempts was used at first and was wrong, because the job gave up for good after a few minutes of outage and stayed failed after Kafka returned (D-023).

## Resubmit from a retained checkpoint

1. Find the latest checkpoint directory of the old job. Each is `file:/opt/flink/checkpoints/<job id>/chk-<n>`:
   ```powershell
   docker exec quakeflow-flink-jobmanager sh -c "ls -d /opt/flink/checkpoints/*/chk-*"
   ```
2. Submit the job with the recovery path set. Put this line before `SET 'pipeline.name'` in the combined SQL:
   ```sql
   SET 'execution.state-recovery.path' = 'file:///opt/flink/checkpoints/<job id>/chk-<n>';
   ```
3. Keep the same transactional id prefixes (they are fixed in `flink/sql/validated_events.sql`), so pending Kafka
   transactions from the old job are recovered.

Without the recovery path the job starts again from the beginning of the raw topic and emits every revision
a second time. The recovery test confirms this: with the checkpoint, 0 repeats, and without it, all of them.

## Spark load interrupted

`batch/ingest_live.py` writes four tables with separate commits, and moves its checkpoint forward only after the whole batch succeeds. A kill therefore leaves the batch unfinished and the tables out of step with each other, and running the job again finishes it. In the test, three kills left the tables in three different states:

| Killed after | Bronze | Silver versions | Silver current |
|---|---|---|---|
| 10 s | 0 | 0 | 0 |
| 17 s | 400 | 0 | 0 |
| 23 s | 400 | 400 | 0 |

After one retry the tables held 400, 400 and 300 rows, with no duplicates and every event's current row being its newest revision. Until the retry finishes, readers can see Bronze ahead of Silver, so treat the lakehouse as consistent only after a load has completed. The `SparkJobNotRunRecently` alert and the `quakeflow_spark_job_success` metric show when a load did not finish.

## Reading the result

Consumers of `quakeflow.events.validated` and `quakeflow.events.dlq` must use `isolation.level=read_committed`.
Verify recovery from the data, not the logs: count distinct `(event_id, record_hash)` pairs and check each appears once.

## Measured results

| Scenario | Result |
|---|---|
| TaskManager killed while 400 distinct revisions were in flight (200 before the kill, 200 sent while it was down, plus repeats) | Restored from a checkpoint 25 s after the kill, which includes an 8 s deliberate outage. All 400 revisions visible after 56 s, each exactly once. |
| Job cancelled, 100 repeats plus 100 new revisions sent, resubmitted from the retained checkpoint | All 200 distinct revisions visible after 42 s, each exactly once, repeats not re-emitted. |
| Same scenario resubmitted without the checkpoint (negative control) | All 100 earlier revisions emitted again, so the tests do detect lost state. |

Reproduce with `python -m pytest -m integration tests/recovery -s` (needs the stack running, takes about 2 minutes).

## Not covered yet

- Killing Kafka while the Flink job runs.
- Corrupt checkpoint files.
- Recovery under load larger than a few hundred messages.
