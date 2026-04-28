# JDK 8 + Maven; multi-arch (linux/amd64, arm64). Older `maven:3.6.3-jdk-8-slim` often fails on ARM with
# "no match for platform in manifest" and is no longer reliably published.
# Default pre-pull in dashboards/demo/k8s/scripts/build-mcac-init-image.sh (override: MCAC_INIT_BASE_IMAGE).
FROM maven:3.9-eclipse-temurin-8 AS builder

# vi for kubectl exec into mcac init/copy pod (optional).
RUN apt-get update \
  && apt-get install -y --no-install-recommends vim-tiny \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /build

COPY . ./
RUN mvn -ff package -DskipTests

# Stage puts agent in /agent so the mcac_data volume mount at /mcac doesn't hide it at run time
RUN mkdir -p /agent/lib
RUN cp /build/target/datastax-mcac-agent-*.jar /agent/lib
RUN cp -r /build/target/collectd /agent/lib

# Populate /mcac (mounted volume) from /agent and exit; avoids running Maven with no goal
WORKDIR /mcac
CMD ["sh", "-c", "mkdir -p /mcac/lib && cp -r /agent/lib/. /mcac/lib/ && exit 0"]
