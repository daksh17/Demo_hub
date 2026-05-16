# Client toolbox for demo-hub: SQL/NoSQL CLIs, curl, OpenSearch CLI, ora2pg.
FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive \
    LANG=C.UTF-8 \
  PATH="/usr/local/bin:${PATH}"

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates curl wget jq dnsutils netcat-openbsd unzip gnupg lsb-release \
    postgresql-client redis-tools perl cpanminus \
    libdbd-pg-perl libdbi-perl     python3 python3-pip python3-venv \
    && rm -rf /var/lib/apt/lists/*

# ora2pg from upstream git (v25.0); Oracle→Postgres needs Instant Client at runtime for DBD::Oracle
RUN apt-get update && apt-get install -y --no-install-recommends \
    git perl make \
    libdbd-pg-perl libdbi-perl libjson-perl libconfig-tiny-perl liblog-log4perl-perl \
    && git clone --depth 1 --branch v25.0 https://github.com/darold/ora2pg.git /tmp/ora2pg \
    && cd /tmp/ora2pg && perl Makefile.PL && make && make install \
    && rm -rf /tmp/ora2pg \
    && rm -rf /var/lib/apt/lists/*

# cqlsh (Cassandra)
RUN pip3 install --no-cache-dir 'cqlsh>=6.1.0'

# mongosh (MongoDB 7.x repo — jammy packages run on noble)
RUN curl -fsSL https://www.mongodb.org/static/pgp/server-7.0.asc \
    | gpg --dearmor -o /usr/share/keyrings/mongodb-server-7.0.gpg \
    && echo "deb [ signed-by=/usr/share/keyrings/mongodb-server-7.0.gpg ] https://repo.mongodb.org/apt/ubuntu jammy/mongodb-org/7.0 multiverse" \
      > /etc/apt/sources.list.d/mongodb-org-7.0.list \
    && apt-get update \
    && apt-get install -y --no-install-recommends mongodb-mongosh \
    && rm -rf /var/lib/apt/lists/*

# OpenSearch CLI (official binary)
ARG OPENSEARCH_CLI_VERSION=1.2.0
RUN curl -fsSL "https://github.com/opensearch-project/opensearch-cli/releases/download/v${OPENSEARCH_CLI_VERSION}/opensearch-cli-${OPENSEARCH_CLI_VERSION}-linux-x64.zip" \
      -o /tmp/opensearch-cli.zip \
    && unzip -q /tmp/opensearch-cli.zip -d /tmp \
    && mv /tmp/opensearch-cli /usr/local/bin/opensearch-cli \
    && chmod +x /usr/local/bin/opensearch-cli \
    && rm -rf /tmp/opensearch-cli.zip /tmp/opensearch-cli

COPY demo-hub-shell-env.sh /etc/profile.d/demo-hub-tools.sh
RUN chmod 644 /etc/profile.d/demo-hub-tools.sh

WORKDIR /demo-hub
CMD ["sleep", "infinity"]
