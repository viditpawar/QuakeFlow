# Flink 2.2.1 on Java 17 plus the matching Kafka SQL connector (built for the Flink 2.2 line).
# Both versions are pinned; the connector download is verified against a SHA-256 checksum.
# To upgrade, change the Flink tag and the connector version together, then update the checksum.
FROM flink:2.3.0-java17

ADD --chown=flink:flink --chmod=644 \
    --checksum=sha256:5605c691d11a501382c383fecba37a7a552467da5ab7ba904ef5d6f3d62c5616 \
    https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/5.0.0-2.2/flink-sql-connector-kafka-5.0.0-2.2.jar \
    /opt/flink/lib/

# Created here, owned by flink, so a named volume mounted on it is writable by the flink user.
RUN mkdir -p /opt/flink/checkpoints && chown flink:flink /opt/flink/checkpoints

# Metrics: Flink 2.x already ships its Prometheus reporter in plugins/, so it only needs to be
# switched on (see docker-compose.yml).

# The job's SQL and the script that submits it again, so a container started by Airflow can recover the job
# without any host path. The compose service still mounts ./flink/sql over the copy.
COPY --chown=flink:flink flink/sql /opt/flink/sql
COPY --chown=flink:flink --chmod=755 flink/resubmit.sh /opt/flink/resubmit.sh
