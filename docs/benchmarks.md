# Benchmarks

What the platform does under synthetic load on one laptop, how that was measured, and what the numbers do not show. Nothing here is estimated: every figure is read from the result files in `benchmarks/results/`, and the tables are produced from them by `python -m benchmarks.run_benchmark --summarize <file>`.

These are the results of the final run, on the setup after both fixes the benchmarks led to: Kafka retention counted from arrival (D-032) and the Spark container raised from 2 GB to 3 GB (D-033). Two earlier runs are kept in `benchmarks/results/before-fix/` and `before-memory-fix/` as evidence of those defects, and are not used for the numbers below.

## The machine

| | |
|---|---|
| CPU | 12th Gen Intel Core i5-12500H, 16 logical processors |
| Docker Desktop | 16 CPUs and 7.6 GB of memory for the virtual machine |
| Host memory | 16 GB, of which only 0.23 to 0.47 GB was free at the lowest point of each run, with the whole platform running |
| Platform | One Kafka broker, 3 partitions per topic, Flink at parallelism 1 with 10 second checkpoints, Spark in a container limited to 3 GB, Iceberg on RustFS |

## What was measured

For each size, on throwaway topics and namespaces, with the real Flink job SQL and the real Spark jobs:

| Stage | How it is timed |
|---|---|
| **Produce** | Sending the messages to the raw topic and waiting for Kafka to confirm all of them |
| **Through Flink** | From the first message produced until every distinct revision is visible to a `read_committed` consumer of the validated topic. Flink only makes output visible when a checkpoint completes, so this includes waiting for the next 10 second checkpoint |
| **Spark ingest** | Wall time of the `ingest_live.py` container, minus 23.7 seconds, which is what the same container takes when there is nothing to read (the container, the JVM, Spark and the catalog starting). That figure is measured once per benchmark |
| **Gold build** | Wall time of the `build_gold.py` container, including its start-up, with all 24 quality checks and the publication |
| **Memory** | The peak, over samples taken every few seconds, of the memory used by all containers together and by each group, and the lowest free memory of the host |

The data: for N revisions, 90 percent are the first version of an event and 10 percent are a second version with a later update time. A further 10 percent of messages are exact repeats of earlier ones, and everything is shuffled, so revisions arrive out of order and Flink has repeats to drop. Magnitudes, coordinates, depths and places are random, and events are spread over a period of up to 45 days. The update times are from 2025 on purpose: that is what lost data before D-032, so every run also checks that the retention fix holds.

**Every run checks its counts before it counts.** After the Gold build the run reads the lakehouse and requires: N rows in Bronze, N revisions in Silver, N times 0.9 events in the current state, no rejected records, Gold's daily counts adding up to the same number of events, and the build recorded as `promoted`. A run that fails any of this is reported as failed and left out of the medians. A failed Spark container keeps its whole output and exit code in `benchmarks/results/failures/`.

## Results

Each size was run twice, so a cell shows the middle of the two runs with both runs' range in brackets. All six runs were correct.

| Revisions | Produce (s) | Produce start to all validated (s) | Flink (per s) | Spark ingest, net (s) | Spark ingest (per s) | Gold build (s) | Peak memory, all containers (MiB) |
|---|---|---|---|---|---|---|---|
| 2,000 | 0.1 (0.1 to 0.1) | 4.0 (3.1 to 4.9) | 528 (404 to 652) | 32.8 (24.7 to 40.8) | 65 (49 to 81) | 61.1 (60.7 to 61.5) | 5,134 (5,123 to 5,145) |
| 10,000 | 0.5 (0.5 to 0.5) | 10.0 (9.0 to 11.0) | 1,007 (906 to 1,109) | 25.6 (24.1 to 27.1) | 392 (369 to 415) | 54.6 (51.4 to 57.8) | 5,093 (5,034 to 5,151) |
| 40,000 | 1.1 (0.9 to 1.2) | 13.6 (10.3 to 17.0) | 3,117 (2,357 to 3,876) | 36.0 (35.3 to 36.7) | 1,112 (1,090 to 1,133) | 62.5 (54.9 to 70.1) | 5,347 (5,316 to 5,377) |

Other figures from the same runs: submitting the Flink job took 13.2 to 23.0 seconds, and the Spark container peaked between 1,919 and 2,396 MiB.

## The 200,000 revision check

One run, not repeated, because it takes about 6 minutes. It is the size that lost data before the retention fix (D-032) and was killed for memory before the Spark limit was raised (D-033), with the same 2025 update times, so passing it is the regression check for both.

| Stage | Result |
|---|---|
| Produce 220,000 messages | 5.1 s |
| Produce start to all validated | 25.8 s, about 7,749 revisions a second (a floor, as above) |
| Spark ingest, net | 149 s, about 1,340 revisions a second |
| Gold build | 64 s |
| Counts | all correct: 200000 in Bronze, 200000 revisions and 180000 events in Silver, none rejected, Gold adding up to 180000 events |
| Peak memory | Spark container 2,668 MiB, all containers 5,456 MiB, host free memory down to 0.16 GB |

What it shows. Both fixes hold at this size. And it is the first run long enough for Spark's rate to stop being dominated by start-up: at about 1,300 revisions a second Spark ingestion is the slowest stage on this setup, against at least 7,700 for Kafka and Flink together. The Spark container used 2,668 MiB, which would not have fitted in 2 GB. The host was down to 0.16 GB free, so this is also close to the most this laptop can do with everything else running.


## Reading the numbers

- **Kafka is not the limit.** Producing 44,000 messages, 40,000 distinct revisions plus the repeats, and getting every one confirmed took about a second.
- **The Flink figures below 40,000 do not measure Flink.** At 2,000 and 10,000 revisions the job finishes inside one or two checkpoint intervals, so "all validated" is decided mostly by where the checkpoint cycle happened to be, and the per second rate is not a throughput. Only 40,000 is long enough to show a rate, 2,400 to 3,900 revisions a second, and that is a floor because the time includes waiting for the commit at the next checkpoint. The real ceiling was not found.
- **Spark ingestion and the Gold build are dominated by fixed cost.** The ingest takes 24 to 41 seconds and the Gold build 51 to 70 seconds at every size from 2,000 to 40,000, which is twenty times the data for about the same time, so the per second rates for Spark rise with size and say little. Only at 200,000 does the ingest take long enough (149 seconds, about 1,300 revisions a second) to show what Spark itself can do.
- **Memory is the tight resource, and it produced the most useful result of these runs.** All containers together peaked at 5.0 to 5.4 GB of the 7.6 GB that Docker has, and the host had as little as 0.23 GB free. The Spark container peaked at up to 2,396 MiB, above the 2 GB limit it had before. At that limit the kernel killed it four times (D-033), including twice in an earlier run of these benchmarks. Running this on a machine with less free memory is likely to fail.
- **These runs are slower than the earlier, pre-fix ones** (Gold 51 to 70 seconds against 38 to 42, Spark start-up 23.7 seconds against 16.6). The host had less free memory this time, which is a plausible reason but was not tested. Compare the two sets with that in mind: the earlier set ran at the 2 GB limit and passed only because it finished before Kafka's retention check.

## What this means for real data

The real feeds are slow compared with these figures. In the first reconciliation run, USGS reported 1,101 events updated over two days, and the live feeds add a handful of events an hour. So the throughput measured here is orders of magnitude above what real data needs, and the delay between a USGS update and its appearance in Gold is set by the schedule, not by the speed: up to 60 seconds for the collector to see it, about 10 seconds to pass Flink, then the wait for the next 30 minute load, then about 2 minutes for the load and the Gold build. That sum is arithmetic from the schedule and these timings, not a measured end-to-end latency, which was not measured.

## What these runs do not show

- **Real data.** The data is uniform and synthetic. Real USGS data comes in bursts after large earthquakes, with fewer revisions and different sizes.
- **Tight statistics.** Two runs per size give a range, not a distribution, and the free memory of the host changed from run to run. A third repeat per size would be cheap and was not done.
- **One configuration.** One broker, one Flink slot, one Spark container with 3 GB. Nothing was tuned, and the Spark heap was left at 1 GB, so these are not the best this setup can do.
- **A cold, empty lakehouse.** Every run starts with empty tables. A Gold build over a Silver table that has been growing for months would take longer, and was not measured.
- **Sustained load, bursts and recovery.** Each run is one batch followed by one load. Nothing ran for hours, and recovery from a failure under load was not measured.
- **The precise cost of each stage.** The Spark start-up is measured once and subtracted from every run, so the net figures are approximate. Memory is sampled every few seconds and can miss a short peak, so the peaks are lower bounds.

## Running it again

Pause the Airflow DAGs first, so nothing else competes for the machine, and have the `core`, `streaming` and `batch` services running.

```powershell
python -m benchmarks.run_benchmark --sizes 2000 10000 40000 --repeats 2
python -m benchmarks.run_benchmark --summarize benchmarks\results\<file>.json
```

Six runs and the 200,000 check took about 27 minutes here. Each run removes its throwaway topics, tables and checkpoints, including when it fails. The results are written after every run, so an interrupted benchmark keeps what it finished.
