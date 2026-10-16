#!/bin/sh
# Submit the streaming job again from the newest retained checkpoint of one earlier job.
#
#     resubmit.sh <job id of the job to resume from>
#
# Run by the flink_health DAG in the Flink image, with the checkpoint volume mounted read-only and the
# JobManager reachable as flink-jobmanager. If a job of this name is already running it does nothing and
# exits 0, so it can never start a second copy. It refuses, with a non-zero exit, when it cannot be sure:
#   3  the given job has no complete checkpoint, and starting from the beginning of the topic would emit every
#      revision a second time. That needs a person to decide.
#   4  the job id is not a Flink job id
set -eu

NAME=quakeflow-validate-and-dedupe
REST=http://flink-jobmanager:8081
CHECKPOINTS=/opt/flink/checkpoints
SQL=/opt/flink/sql

OLD_JOB="${1:-}"
case "$OLD_JOB" in
  ''|*[!0-9a-f]*) echo "resubmit: '$OLD_JOB' is not a job id" >&2; exit 4 ;;
esac
[ "${#OLD_JOB}" -eq 32 ] || { echo "resubmit: '$OLD_JOB' is not a job id" >&2; exit 4; }

# Look again right before submitting: a person may have submitted it in the meantime.
JOBS=$(curl -sf "$REST/jobs/overview") || { echo "resubmit: JobManager not reachable" >&2; exit 1; }
if printf '%s' "$JOBS" | grep -oE "\"name\":\"$NAME\",\"start-time\":[0-9-]+,\"end-time\":[0-9-]+,\"duration\":[0-9-]+,\"state\":\"[A-Z_]+\"" \
     | grep -qE 'RUNNING|RESTARTING|CREATED|INITIALIZING|RECONCILING'; then
  echo "resubmit: $NAME is already running, nothing to do" >&2
  exit 0
fi

# The newest checkpoint of that job that has its metadata file, which Flink writes last.
CHECKPOINT=""
# Sorted by the checkpoint number alone, so nothing else in the path can change the order.
for dir in $(for d in "$CHECKPOINTS/$OLD_JOB"/chk-*; do [ -d "$d" ] && echo "${d##*chk-} $d"; done | sort -n -r | cut -d' ' -f2); do
  if [ -f "$dir/_metadata" ]; then CHECKPOINT="$dir"; break; fi
done
if [ -z "$CHECKPOINT" ]; then
  echo "resubmit: job $OLD_JOB has no complete checkpoint, not starting from the beginning on my own" >&2
  exit 3
fi
echo "resubmit: resuming from file://$CHECKPOINT" >&2

JOB_SQL=/tmp/job.sql
{
  cat "$SQL/kafka_sources.sql" "$SQL/validated_events.sql"
  echo "SET 'execution.state-recovery.path' = 'file://$CHECKPOINT';"
  cat "$SQL/revision_tracking.sql"
} > "$JOB_SQL"

exec /opt/flink/bin/sql-client.sh -Drest.address=flink-jobmanager -Drest.port=8081 -f "$JOB_SQL"
