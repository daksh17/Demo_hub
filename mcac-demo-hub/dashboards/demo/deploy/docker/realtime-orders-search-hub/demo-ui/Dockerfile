FROM python:3.11-slim
WORKDIR /app
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
     freetds-bin \
     vim-tiny \
  && rm -rf /var/lib/apt/lists/*
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY app.py scenario.py hub_config.py postgres_logical_demo.py postgres_faker_schema.py postgres_schema_clone.py kafka_lab.py .
EXPOSE 8888
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8888"]
