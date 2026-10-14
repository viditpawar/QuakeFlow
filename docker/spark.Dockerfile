# Spark 4.0.4 on Java 17 with Iceberg 1.12.0 (Spark 4.0 runtime), its S3 bundle, and the PostgreSQL
# JDBC driver for the Iceberg catalog. Every download is pinned and verified against a SHA-256.
# To upgrade, change the Spark tag and the Iceberg runtime together (the runtime name carries the
# Spark line, 4.0_2.13), then update the checksums.
FROM apache/spark:4.0.4-scala2.13-java17-python3-ubuntu

USER root

ADD --chmod=644 \
    --checksum=sha256:3f50e93e8ed322ae0da0e380f1b6c27116404329c62e55f0586697b1b8378306 \
    https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-4.0_2.13/1.12.0/iceberg-spark-runtime-4.0_2.13-1.12.0.jar \
    /opt/spark/jars/
ADD --chmod=644 \
    --checksum=sha256:dd7e49ffa4ac9b215bed846581991259e3aec9bb2fa08d6cec029c46cd66d957 \
    https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.12.0/iceberg-aws-bundle-1.12.0.jar \
    /opt/spark/jars/
ADD --chmod=644 \
    --checksum=sha256:73914527305a40cce504b0d3d90b23caf565136912d607ae8ae7c5895512332c \
    https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.14/postgresql-42.7.14.jar \
    /opt/spark/jars/

# Kafka source for Structured Streaming. The connector needs kafka-clients and commons-pool2,
# whose versions are the ones its own pom declares for Spark 4.0.4.
ADD --chmod=644 \
    --checksum=sha256:b2f7b3a4fbf292b5bf10bd4dd6af301eded9c9985b16239b407401d66dbc8a25 \
    https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.13/4.0.4/spark-sql-kafka-0-10_2.13-4.0.4.jar \
    /opt/spark/jars/
ADD --chmod=644 \
    --checksum=sha256:ae9817053af94a21992e948269eba1a50d511333cb51462865983abfbeea7f60 \
    https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.13/4.0.4/spark-token-provider-kafka-0-10_2.13-4.0.4.jar \
    /opt/spark/jars/
ADD --chmod=644 \
    --checksum=sha256:7568b998572d256f0b7bc0afdc1b7a2588b8b08415c62ce314c864a6851ae9d9 \
    https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.9.1/kafka-clients-3.9.1.jar \
    /opt/spark/jars/
ADD --chmod=644 \
    --checksum=sha256:6d3bd18df8410f3e31b031aca582cc109342358a62a2759ebd0c4cdf30d06f8b \
    https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.12.0/commons-pool2-2.12.0.jar \
    /opt/spark/jars/

# The image's Python is 3.10, older than the 3.11 used on the host, so code that runs here
# (batch/*.py except backfill.py) must not use 3.11-only features. PyYAML reads the quality
# contract. It is pinned and verified against the SHA-256 PyPI publishes for this wheel.
RUN --mount=type=bind,target=/tmp/req,source=docker/spark-requirements.txt \
    python3 -m pip install --no-cache-dir --require-hashes -r /tmp/req

# Structured Streaming checkpoints live here (a named volume), owned by the spark user.
RUN mkdir -p /checkpoints && chown spark:spark /checkpoints

# Renders the catalog configuration from environment variables at start-up, so credentials never
# live in an image layer or in the repository.
COPY --chmod=755 docker/spark-entrypoint.sh /opt/quakeflow/spark-entrypoint.sh

USER spark
ENTRYPOINT ["/opt/quakeflow/spark-entrypoint.sh"]
