# syntax=docker/dockerfile:1
# Apache NiFi 2.12 plus what the convenience image lacks: Parquet record writers (two NARs that
# sit in an inactive build profile upstream) and the PostgreSQL JDBC driver. Every artifact comes
# from Maven Central and is checked against the sha256 below, recorded at first download and
# cross-checked with Maven's published .sha1 (docs/PLAN.md 6.1).

ARG NIFI_IMAGE=apache/nifi:2.12.0@sha256:6f7f2840dbf178c6dd3fa13435b9b358f3be3c6fb7417de467a35a267f4d28ad

# The base image already has curl and sha256sum, so it doubles as the downloader.
FROM ${NIFI_IMAGE} AS download
ARG MAVEN=https://repo.maven.apache.org/maven2
ARG NIFI_VERSION=2.12.0
ARG PGJDBC_VERSION=42.7.14
ARG PGJDBC_SHA256=73914527305a40cce504b0d3d90b23caf565136912d607ae8ae7c5895512332c
ARG PARQUET_NAR_SHA256=b177d80e4c1564ad7db1837256971a8c512a9c15afecc21ece724aab84af1af2
ARG HADOOP_LIBS_NAR_SHA256=c6496f21a24276070b1db28bac5ded004f21549e00fd143013524ff4e96449be
WORKDIR /tmp/dl
RUN set -eu; \
    curl -fsSLO "${MAVEN}/org/postgresql/postgresql/${PGJDBC_VERSION}/postgresql-${PGJDBC_VERSION}.jar"; \
    curl -fsSLO "${MAVEN}/org/apache/nifi/nifi-parquet-nar/${NIFI_VERSION}/nifi-parquet-nar-${NIFI_VERSION}.nar"; \
    curl -fsSLO "${MAVEN}/org/apache/nifi/nifi-hadoop-libraries-nar/${NIFI_VERSION}/nifi-hadoop-libraries-nar-${NIFI_VERSION}.nar"; \
    printf '%s  %s\n' \
        "${PGJDBC_SHA256}" "postgresql-${PGJDBC_VERSION}.jar" \
        "${PARQUET_NAR_SHA256}" "nifi-parquet-nar-${NIFI_VERSION}.nar" \
        "${HADOOP_LIBS_NAR_SHA256}" "nifi-hadoop-libraries-nar-${NIFI_VERSION}.nar" \
        > SHA256SUMS; \
    sha256sum -c SHA256SUMS

FROM ${NIFI_IMAGE}
# NARs in lib/ load at boot, before the flow, so there is no autoload race.
COPY --from=download --chown=nifi:nifi /tmp/dl/*.nar /opt/nifi/nifi-current/lib/
COPY --from=download --chown=nifi:nifi /tmp/dl/*.jar /opt/nifi/nifi-current/jdbc/
USER nifi
# TCP only: no curl or token needed; `retail-pricing nifi deploy` checks the API itself. The image
# binds HTTPS to the container hostname (NIFI_WEB_HTTPS_HOST), not to 127.0.0.1.
HEALTHCHECK --interval=10s --timeout=5s --retries=30 --start-period=30s \
    CMD ["bash", "-c", "</dev/tcp/$(hostname)/8443"]
