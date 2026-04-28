#!/usr/bin/env bash
# Start the full MCAC demo: Cassandra (all rings), Kafka/Connect, Postgres HA, Mongo sharded,
# Redis, OpenSearch, Prometheus, Grafana, exporters — everything in ./docker-compose.yml.
#
# Performs a clean restart: compose down + removal of stray containers that would cause name
# conflicts (orphans, legacy fixed names). Named volumes are kept unless CLEAN_VOLUMES=1.
#
# Do NOT use ../../docker-compose.yaml (dashboards root); that file only runs Prometheus + Grafana.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
cd "${SCRIPT_DIR}"

# Aligns with compose file top-level `name: demo-hub`. Override if needed (e.g. COMPOSE_PROJECT_NAME=demo
# reuses older Docker volume names like demo_postgres_primary_data).
COMPOSE_PROJECT_NAME="${COMPOSE_PROJECT_NAME:-demo-hub}"
export COMPOSE_PROJECT_NAME

if [[ -z "${PROJECT_VERSION:-}" ]]; then
  PROJECT_VERSION="$(grep '<revision>' "${REPO_ROOT}/pom.xml" | sed -E 's/(<\/?revision>|[[:space:]])//g')"
  export PROJECT_VERSION
fi
if [[ -z "${PROJECT_VERSION}" ]]; then
  echo "error: could not read <revision> from ${REPO_ROOT}/pom.xml; set PROJECT_VERSION manually." >&2
  exit 1
fi

# Fail fast with a clear message when the engine is down (OrbStack often shows: error during connect ... EOF).
if ! docker info >/dev/null 2>&1; then
  echo "error: Docker engine is not reachable (docker info failed)." >&2
  echo "  Fix OrbStack first — nothing in this script can run until Docker answers." >&2
  echo "  Typical recovery: quit OrbStack completely → wait 30s → open OrbStack again; or reboot; free disk space." >&2
  echo "  Broken CLI plugin warnings under ~/.docker/cli-plugins/ are unrelated; EOF on the socket means the daemon is down." >&2
  exit 1
fi

_remove_stray_containers() {
  # Remove orphans that still hold "{{project}}-*" names. Compose lowercases the project segment.
  local -a raw=() to_rm=()
  local id name part prefix suffix proj_lc x hit dup
  proj_lc="$(echo "${COMPOSE_PROJECT_NAME}" | tr '[:upper:]' '[:lower:]')"
  prefix="${proj_lc}-"
  suffix="_${proj_lc}-"

  # Substring filter on container name (reliable on OrbStack/Linux).
  while read -r id; do
    [[ -z "${id}" ]] && continue
    raw+=("${id}")
  done < <(docker ps -aq --filter "name=${prefix}" 2>/dev/null || true)

  # {{.Names}} may be comma-separated aliases; strip leading /.
  while read -r id name; do
    [[ -z "${id}" ]] && continue
    name="${name#/}"
    hit=0
    IFS=',' read -ra parts <<< "${name}"
    for part in "${parts[@]}"; do
      part="${part#/}"
      part="$(echo "${part}" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')"
      if [[ "${part}" == "${prefix}"* ]] || [[ "${part}" == *"${suffix}"* ]]; then
        hit=1
        break
      fi
    done
    if [[ "${hit}" -eq 1 ]]; then
      raw+=("${id}")
    fi
  done < <(docker ps -a --format '{{.ID}} {{.Names}}' 2>/dev/null || true)

  for id in "${raw[@]}"; do
    dup=0
    for x in "${to_rm[@]}"; do
      if [[ "${x}" == "${id}" ]]; then
        dup=1
        break
      fi
    done
    if [[ "${dup}" -eq 0 ]]; then
      to_rm+=("${id}")
    fi
  done

  if ((${#to_rm[@]} > 0)); then
    echo "  force-removing ${#to_rm[@]} stray container id(s) matching ${prefix}* ..."
    docker rm -f "${to_rm[@]}" 2>/dev/null || true
  else
    echo "  no stray ${prefix}* containers found."
  fi
}

echo "COMPOSE_PROJECT_NAME=${COMPOSE_PROJECT_NAME}"
echo "PROJECT_VERSION=${PROJECT_VERSION}"

if [[ "${CLEAN_VOLUMES:-0}" == "1" ]]; then
  echo "Stopping stack and removing containers + anonymous volumes (CLEAN_VOLUMES=1)..."
  docker compose down --remove-orphans -v --timeout 120
else
  echo "Stopping stack and removing containers (named volumes preserved; set CLEAN_VOLUMES=1 to wipe them)..."
  echo "  (if this hangs >2–3 min, restart OrbStack/Docker Desktop, then retry)"
  docker compose down --remove-orphans --timeout 120
fi

# Legacy: Prometheus used container_name: prometheus-mcac (fixed global name).
docker rm -f prometheus-mcac >/dev/null 2>&1 || true

echo "Removing stray ${COMPOSE_PROJECT_NAME}-* containers (orphans / failed partial ups)..."
_remove_stray_containers
echo "Cleanup finished."

echo "Building mcac (agent into volume), kafka-connect (Mongo sink), hub-demo-ui, and postgresql-repmgr..."
docker compose build mcac kafka-connect hub-demo-ui postgresql-primary

echo "Starting all services (this may take several minutes)..."
# Re-run stray cleanup after build (long gap: other terminals / half-dead orphans can race names).
echo "Final stray-name cleanup before up..."
_remove_stray_containers
set +e
docker compose up -d
up_rc=$?
set -e
if [[ "${up_rc}" -ne 0 ]]; then
  echo "docker compose up failed (often a leftover name); retrying once after stray cleanup..."
  _remove_stray_containers
  docker compose up -d
fi

echo ""
echo "Full stack is starting. When healthy:"
echo "  Grafana    http://localhost:3000"
echo "  Prometheus http://localhost:9090"
echo "  Kafka      localhost:9092   Connect REST http://localhost:8083"
echo "  Postgres primary localhost:15432  Mongo mongos localhost:27025"
echo "  Redis      localhost:6379   OpenSearch http://localhost:9200"
echo "  Hub demo UI http://localhost:8888  (writes Postgres/Mongo/Redis/Cassandra/OpenSearch)"
echo ""
echo "Kafka Connect demo connectors (pg + mongo CDC/sink) register via one-shot service **kafka-connect-register**."
echo "Check: docker compose logs kafka-connect-register   |   curl -s http://localhost:8083/connectors"
echo "Re-run manually if needed: ./deploy/docker/kafka-connect-register/register-all.sh"
