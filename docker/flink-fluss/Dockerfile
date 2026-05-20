FROM docker.io/library/flink:1.20

# Superset image: Flink 1.20 + Paimon + Apache Fluss + S3 plugin.
# Used by every Flink container in the workshop from scenario 06 onward;
# scenarios 01-05 don't touch Paimon/Fluss code paths so they are unaffected
# (the extra jars on the classpath are inert if nothing references them).
#
# Versions are pinned to match root pom.xml properties:
#   <flink.version>1.20.4</flink.version>
#   <paimon.version>1.0.1</paimon.version>
#   <fluss.version>0.9.1</fluss.version>

ARG PAIMON_VERSION=1.0.1
ARG FLINK_S3_VERSION=1.20.4
ARG FLUSS_VERSION=0.9.1

USER root

RUN set -eux; \
    # ── Paimon (scenario 06) ──────────────────────────────────────────
    curl -fsSL -o /opt/flink/lib/paimon-flink-1.20-${PAIMON_VERSION}.jar \
        https://repo.maven.apache.org/maven2/org/apache/paimon/paimon-flink-1.20/${PAIMON_VERSION}/paimon-flink-1.20-${PAIMON_VERSION}.jar; \
    curl -fsSL -o /opt/flink/lib/paimon-s3-${PAIMON_VERSION}.jar \
        https://repo.maven.apache.org/maven2/org/apache/paimon/paimon-s3/${PAIMON_VERSION}/paimon-s3-${PAIMON_VERSION}.jar; \
    # ── Apache Fluss (scenarios 11-16) ────────────────────────────────
    # The Fluss connector for Flink is published to Maven Central under
    # org.apache.fluss:fluss-flink-1.20. The "incubating" classifier is
    # part of the version string.
    curl -fsSL -o /opt/flink/lib/fluss-flink-1.20-${FLUSS_VERSION}-incubating.jar \
        https://repo.maven.apache.org/maven2/org/apache/fluss/fluss-flink-1.20/${FLUSS_VERSION}-incubating/fluss-flink-1.20-${FLUSS_VERSION}-incubating.jar; \
    # Fluss tiering connector to Paimon (scenario 16)
    curl -fsSL -o /opt/flink/lib/fluss-lake-paimon-${FLUSS_VERSION}-incubating.jar \
        https://repo.maven.apache.org/maven2/org/apache/fluss/fluss-lake-paimon/${FLUSS_VERSION}-incubating/fluss-lake-paimon-${FLUSS_VERSION}-incubating.jar || true; \
    # ── S3 filesystem plugin ──────────────────────────────────────────
    mkdir -p /opt/flink/plugins/s3-fs-hadoop; \
    curl -fsSL -o /opt/flink/plugins/s3-fs-hadoop/flink-s3-fs-hadoop-${FLINK_S3_VERSION}.jar \
        https://repo.maven.apache.org/maven2/org/apache/flink/flink-s3-fs-hadoop/${FLINK_S3_VERSION}/flink-s3-fs-hadoop-${FLINK_S3_VERSION}.jar; \
    chown -R flink:flink /opt/flink/lib /opt/flink/plugins

USER flink
