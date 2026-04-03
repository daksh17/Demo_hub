# Cassandra (MCAC demo cluster)

This folder documents the **Apache Cassandra** side of the single-stack compose in **`../docker-compose.yml`**. It does **not** define services by itself—the cluster, the MCAC agent image, and **nodetool-exporter** are all declared in that file.

## What lives here vs the repo root

| Item | Location |
|------|----------|
| Compose service definitions | `../docker-compose.yml` |
| **nodetool-exporter** image build context | `../nodetool-exporter/` (`Dockerfile`, `exporter.py`) |
| MCAC agent / collector config | Repo `../../config/`, agent JAR from `../../pom.xml` **`PROJECT_VERSION`** |
| Prometheus file SD for MCAC | `../tg_mcac.json` (mounted into Prometheus) |

## Services (names to use in `docker compose`)

- **`mcac`** — builds the metric collector agent from the project root; shared volume **`mcac_data`** supplies the JVM agent under `/mcac`.
- **`cassandra`**, **`cassandra2`**, … — see the cluster / DC / rack table in **[`../README.md`](../README.md)** (“Cluster, datacenter and rack”).
- **`nodetool-exporter`** — HTTP **`9104`**; scrapes `nodetool` on the configured **`CASSANDRA_HOSTS`** and exposes Prometheus metrics.

## Typical commands

From **`dashboards/demo`**:

```bash
export PROJECT_VERSION=$(grep '<revision>' ../../pom.xml | sed -E 's/(<\/?revision>|[[:space:]])//g')
docker compose up -d cassandra cassandra2 cassandra3 nodetool-exporter prometheus grafana
```

`nodetool` via Docker (replace host with a running Cassandra container name):

```bash
docker compose exec cassandra nodetool status
```

## Further reading

- Full demo walkthrough, stress, and topology: **[`../README.md`](../README.md)**.
- Dashboards JSON: `../../grafana/generated-dashboards/`.
- Regenerate Grafana boards after jsonnet edits: `../../grafana/make-dashboards.sh`.
