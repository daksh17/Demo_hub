FROM cassandra:4.0 AS cass_tools
# Bookworm: OpenJDK 17 still exists (trixie/current slim only ships newer JDKs). Cassandra 4.0
# nodetool expects Java 11–17; Java 21 breaks reflective access in cassandra-shaded tools.
FROM python:3.11-slim-bookworm
WORKDIR /app
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
     openjdk-17-jre-headless \
     procps \
     ca-certificates \
     curl \
     freetds-bin \
     vim-tiny \
  && ln -sfn "/usr/lib/jvm/java-17-openjdk-$(dpkg --print-architecture)" /opt/jdk17 \
  && KARCH="$(dpkg --print-architecture)" \
  && curl -fsSL -o /usr/local/bin/kubectl \
     "https://dl.k8s.io/release/v1.29.6/bin/linux/${KARCH}/kubectl" \
  && chmod +x /usr/local/bin/kubectl \
  && rm -rf /var/lib/apt/lists/*
COPY --from=cass_tools /opt/cassandra /opt/cassandra
# Official image: /opt/cassandra/conf -> /etc/cassandra (symlink). COPY of /opt/cassandra alone
# does not include that target, so nodetool would miss jvm*.options until /etc/cassandra exists here too.
COPY --from=cass_tools /etc/cassandra /etc/cassandra
# logs -> /var/log/cassandra; copy that tree so mkdir / log writes do not fight a dangling symlink.
COPY --from=cass_tools /var/log/cassandra /var/log/cassandra
RUN set -eux \
  && sz="$(du -sk /opt/cassandra | cut -f1)" \
  && test "${sz}" -gt 10000 \
  && chmod -R a+rX /opt/cassandra /etc/cassandra /var/log/cassandra \
  && chmod +x /opt/cassandra/bin/nodetool \
  && test -f /opt/cassandra/conf/jvm-clients.options \
  && test -f /opt/cassandra/conf/jvm11-clients.options \
  && test -f /opt/cassandra/bin/nodetool \
  && test -x /opt/cassandra/bin/nodetool
ENV JAVA_HOME=/opt/jdk17
ENV CASSANDRA_HOME=/opt/cassandra
ENV PATH="/opt/cassandra/bin:/opt/jdk17/bin:${PATH}"
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY app.py scenario.py hub_config.py postgres_logical_demo.py postgres_faker_schema.py postgres_partition_demo.py postgres_schema_clone.py kafka_lab.py cassandra_storage_lab.py cassandra_partition_lab.py cassandra_nodetool_lab.py cassandra_consistency_lab.py .
EXPOSE 8888
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8888"]
