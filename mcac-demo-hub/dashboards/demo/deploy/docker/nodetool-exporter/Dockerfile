# Use Cassandra image so nodetool is available; add Python and prometheus_client
FROM cassandra:4.0
RUN apt-get update && apt-get install -y --no-install-recommends python3 python3-pip python3-venv vim-tiny && rm -rf /var/lib/apt/lists/*
# Use venv to avoid PEP 668 "externally-managed-environment" on Python 3.11+
RUN python3 -m venv /app/venv && /app/venv/bin/pip install --upgrade pip prometheus_client
WORKDIR /app
COPY exporter.py /app/
EXPOSE 9104
ENV NODETOOL_EXPORTER_PORT=9104
CMD ["/app/venv/bin/python3", "/app/exporter.py"]
