FROM docker.io/apache/spark:3.5.5-java17

# Workshop Spark image: Spark 3.5.5 (Scala 2.12) + Paimon Spark connector +
# Fluss Spark connector. Used by scenario-16 for batch analytics against
# Paimon-tiered Fluss data and live Fluss reads.
#
# Versions pinned to:
#   spark    3.5.5  (Scala 2.12)
#   paimon   1.0.1
#   fluss    0.9.1-incubating

ARG PAIMON_VERSION=1.0.1
ARG FLUSS_VERSION=0.9.1
ARG HADOOP_AWS_VERSION=3.3.4
ARG AWS_SDK_VERSION=1.12.262

USER root

RUN set -eux; \
    SPARK_JARS=/opt/spark/jars; \
    # ── Paimon for Spark 3.5 ───────────────────────────────────────────
    curl -fsSL -o ${SPARK_JARS}/paimon-spark-3.5-${PAIMON_VERSION}.jar \
        https://repo.maven.apache.org/maven2/org/apache/paimon/paimon-spark-3.5/${PAIMON_VERSION}/paimon-spark-3.5-${PAIMON_VERSION}.jar; \
    curl -fsSL -o ${SPARK_JARS}/paimon-s3-${PAIMON_VERSION}.jar \
        https://repo.maven.apache.org/maven2/org/apache/paimon/paimon-s3/${PAIMON_VERSION}/paimon-s3-${PAIMON_VERSION}.jar; \
    # ── Fluss connector for Spark 3.5 ──────────────────────────────────
    curl -fsSL -o ${SPARK_JARS}/fluss-spark-3.5-${FLUSS_VERSION}-incubating.jar \
        https://repo.maven.apache.org/maven2/org/apache/fluss/fluss-spark-3.5/${FLUSS_VERSION}-incubating/fluss-spark-3.5-${FLUSS_VERSION}-incubating.jar || true; \
    # ── S3A filesystem support for MinIO ──────────────────────────────
    curl -fsSL -o ${SPARK_JARS}/hadoop-aws-${HADOOP_AWS_VERSION}.jar \
        https://repo.maven.apache.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar; \
    curl -fsSL -o ${SPARK_JARS}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar \
        https://repo.maven.apache.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar; \
    chown -R spark:spark ${SPARK_JARS}

# Pre-configure spark-defaults so spark-submit Just Works against the
# workshop's MinIO + Paimon warehouse.
RUN cat <<'CONF' > /opt/spark/conf/spark-defaults.conf
spark.sql.extensions                                org.apache.paimon.spark.extensions.PaimonSparkSessionExtension
spark.sql.catalog.paimon                            org.apache.paimon.spark.SparkCatalog
spark.sql.catalog.paimon.warehouse                  s3a://paimon/warehouse
spark.sql.catalog.paimon.metastore                  filesystem

spark.hadoop.fs.s3a.endpoint                        http://workshop-minio:9000
spark.hadoop.fs.s3a.access.key                      workshop
spark.hadoop.fs.s3a.secret.key                      workshopsecret
spark.hadoop.fs.s3a.path.style.access               true
spark.hadoop.fs.s3a.connection.ssl.enabled          false
spark.hadoop.fs.s3a.aws.credentials.provider        org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider

# Long-running container — keep the driver alive so podman exec spark-sql works
spark.driver.host                                   workshop-spark
CONF

USER spark
WORKDIR /opt/spark

# Default command: idle so `podman exec workshop-spark spark-submit ...` can run.
CMD ["tail", "-f", "/dev/null"]
