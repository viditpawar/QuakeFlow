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

# Renders the catalog configuration from environment variables at start-up, so credentials never
# live in an image layer or in the repository.
COPY --chmod=755 docker/spark-entrypoint.sh /opt/quakeflow/spark-entrypoint.sh

USER spark
ENTRYPOINT ["/opt/quakeflow/spark-entrypoint.sh"]
