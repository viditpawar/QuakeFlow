# Benchmarks

> **Status: not final.** The tables below were measured on 2026-10-16 **before** the Kafka retention fix (D-032), and their runs finished before the broker's retention check, which is why they passed. A rerun on the fixed setup was started. At the time of writing it had 7 of 9 runs finished, and **2 of them failed** (a Spark ingestion container exited with a non-zero code, cause not yet found), so these numbers are not yet confirmed and should not be quoted. This banner goes when the rerun is analysed. See `tasks.md`.

What the platform does under synthetic load on one laptop, how that was measured, and what the numbers do not show. Nothing here is estimated: every figure is read from the result files in `benchmarks/results/`, and the table is produced from them by `python -m benchmarks.run_benchmark --summarize <file>`.

## The machine

| | |
|---|---|
| CPU | 12th Gen Intel Core i5-12500H, 16 logical processors |
| Docker Desktop | 16 CPUs and 7.6 GB of memory for the virtual machine |
| Host memory | 16 GB. Between 0.5 and 1.8 GB of it was free during the runs, with the whole platform running |
| Platform | One Kafka broker, 3 partitions per topic, Flink at parallelism 1 with 10 second checkpoints, Spark in a container limited to 2 GB, Iceberg on RustFS |

## What was measured

For each size, on throwaway topics and namespaces, with the real Flink job SQL and the real Spark jobs:

| Stage | How it is timed |
|---|---|
| **Produce** | Sending the messages to the raw topic and waiting for Kafka to confirm all of them |
| **Through Flink** | From the first message produced until every distinct revision is visible to a `read_committed` consumer of the validated topic. Flink only makes output visible when a checkpoint completes, so this includes waiting for the next 10 second checkpoint |
| **Spark ingest** | Wall time of the `ingest_live.py` container, minus 16.6 seconds, which is what the same container takes when there is nothing to read (the container, the JVM, Spark and the catalog starting). That figure is measured once per benchmark |
| **Gold build** | Wall time of the `build_gold.py` container, including its start-up, with all 24 quality checks and the publication |
| **Memory** | The peak, over samples taken every few seconds, of the memory used by all containers together and by each group, and the lowest free memory of the host |

The data: for N revisions, 90 percent are the first version of an event and 10 percent are a second version with a later update time. A further 10 percent of messages are exact repeats of earlier ones, and everything is shuffled, so revisions arrive out of order and Flink has repeats to drop. Magnitudes, coordinates, depths and places are random, and events are spread over a period of up to 45 days.

**Every run checks its counts before it counts.** After the Gold build the run reads the lakehouse and requires: N rows in Bronze, N revisions in Silver, N times 0.9 events in the current state, no rejected records, Gold's daily counts adding up to the same number of events, and the build recorded as `promoted`. A run that fails any of this is reported as failed and left out of the medians.

## Results

Each size was run three times. Medians, with the lowest and highest run in brackets. All nine runs were correct.

| Revisions | Produce (s) | Produce start to all validated (s) | Flink (per s) | Spark ingest, net (s) | Spark ingest (per s) | Gold build (s) | Peak memory, all containers (MiB) |
|---|---|---|---|---|---|---|---|
| 2,000 | 0.0 (0.0 to 0.0) | 5.4 (0.7 to 8.3) | 370 (242 to 2,778) | 19.0 (14.5 to 24.2) | 105 (83 to 138) | 37.6 (36.8 to 45.9) | 4,936 (4,832 to 5,036) |
| 10,000 | 0.2 (0.1 to 0.2) | 3.5 (3.0 to 8.2) | 2,890 (1,217 to 3,367) | 19.7 (19.5 to 32.1) | 508 (312 to 513) | 39.0 (38.1 to 39.5) | 5,010 (4,962 to 5,029) |
| 40,000 | 0.3 (0.3 to 0.4) | 6.5 (6.3 to 6.8) | 6,202 (5,917 to 6,369) | 24.5 (24.0 to 26.0) | 1,633 (1,538 to 1,667) | 41.9 (40.0 to 42.0) | 4,987 (4,925 to 5,017) |

Other figures from the same runs: submitting the Flink job took 11.0 to 12.3 seconds, and the Spark container used between 1,834 and 2,048 MiB of its 2,048 MiB limit at its peak.

## Reading the numbers

- **Kafka is not the limit.** Producing 44,000 messages, 40,000 distinct revisions plus the repeats, and getting every one confirmed took 0.3 to 0.4 seconds.
- **The Flink figures below 40,000 do not measure Flink.** At 2,000 and 10,000 revisions the whole job finishes inside one or two checkpoint intervals, so "all validated" is decided by where the checkpoint cycle happened to be (0.7 to 8.3 seconds), and the per second rate is meaningless. Only the 40,000 runs are long enough to show a rate, which is about 6,000 revisions a second. That is a floor, because the time includes waiting for the commit at the next checkpoint. The real ceiling was not found by these runs.
- **Spark ingestion and the Gold build are dominated by fixed cost.** The ingest takes 15 to 32 seconds at every size from 2,000 to 40,000, and the Gold build 37 to 46 seconds. Twenty times more data added only about 5 seconds to the ingest and 4 to the Gold build, so the per second rates for Spark rise with size and say little about its speed. They show that the cost of running a job, not of the data, is what this setup pays at these sizes.
- **Memory is the tight resource.** With everything running, the containers together peaked at about 5.0 GB, of the 7.6 GB that Docker has. The Spark container reached its 2 GB limit in one run, and the host had as little as 0.46 GB free. Running the benchmark with the Airflow DAGs active, or anything else large, is likely to run the machine out of memory, which is why the DAGs were paused during it.

## What this means for real data

The real feeds are slow compared with these figures. In the first reconciliation run, USGS reported 1,101 events updated over two days, and the live feeds add a handful of events an hour. So the throughput measured here is orders of magnitude above what real data needs, and the delay between a USGS update and its appearance in Gold is set by the schedule, not by the speed: up to 60 seconds for the collector to see it, about 10 seconds to pass Flink, then the wait for the next 30 minute load, then about 1.5 minutes for the load and the Gold build. That sum is arithmetic from the schedule and these timings, not a measured end-to-end latency, which was not measured.

## What these runs do not show

- **Real data.** The data is uniform and synthetic. Real USGS data comes in bursts after large earthquakes, with fewer revisions and different sizes.
- **One configuration.** One broker, one Flink slot, one Spark container with 2 GB. Nothing was tuned, and no other setting was tried, so these are not the best this setup can do.
- **A cold, empty lakehouse.** Every run starts with empty tables. A Gold build over a Silver table that has been growing for months would take longer than it did here, and was not measured.
- **Sustained load, bursts and recovery.** Each run is one batch followed by one load. Nothing was run for hours, and recovery from a failure under load was not measured.
- **Statistics.** Three runs per size give a median and a range, not a confidence interval. The sampling of memory, every few seconds, can miss a short peak, so the peaks are lower bounds.
- **The precise cost of each stage.** The Spark start-up is measured once and subtracted from every run, so the net figures are approximate.

## Running it again

Pause the Airflow DAGs first, so nothing else competes for the machine, and have the `core`, `streaming` and `batch` services running.

```powershell
python -m benchmarks.run_benchmark --sizes 2000 10000 40000 --repeats 3
python -m benchmarks.run_benchmark --summarize benchmarks\results\<file>.json
```

Three repeats of three sizes took about 38 minutes here. Each run removes its throwaway topics, tables and checkpoints, including when it fails. The results are written after every run, so a long benchmark that is interrupted keeps what it finished.
