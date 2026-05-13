FROM docker.io/library/flink:1.20

# Versions are pinned to match the workshop's flink.version (1.20.4) and
# paimon.version (1.0.1) declared in the root pom.xml. Bump both together.
ARG PAIMON_VERSION=1.0.1
ARG FLINK_S3_VERSION=1.20.4

USER root

RUN set -eux; \
    curl -fsSL -o /opt/flink/lib/paimon-flink-1.20-${PAIMON_VERSION}.jar \
        https://repo.maven.apache.org/maven2/org/apache/paimon/paimon-flink-1.20/${PAIMON_VERSION}/paimon-flink-1.20-${PAIMON_VERSION}.jar; \
    curl -fsSL -o /opt/flink/lib/paimon-s3-${PAIMON_VERSION}.jar \
        https://repo.maven.apache.org/maven2/org/apache/paimon/paimon-s3/${PAIMON_VERSION}/paimon-s3-${PAIMON_VERSION}.jar; \
    mkdir -p /opt/flink/plugins/s3-fs-hadoop; \
    curl -fsSL -o /opt/flink/plugins/s3-fs-hadoop/flink-s3-fs-hadoop-${FLINK_S3_VERSION}.jar \
        https://repo.maven.apache.org/maven2/org/apache/flink/flink-s3-fs-hadoop/${FLINK_S3_VERSION}/flink-s3-fs-hadoop-${FLINK_S3_VERSION}.jar; \
    chown -R flink:flink /opt/flink/lib /opt/flink/plugins

USER flink
