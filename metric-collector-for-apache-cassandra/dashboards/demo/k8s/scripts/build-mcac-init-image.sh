#!/usr/bin/env bash
# Build the MCAC agent image used by the Cassandra StatefulSet initContainer (not published to Docker Hub).
#
# From repo root this is: docker build -t mcac-demo/mcac-init:local -f Dockerfile .
# Run this script from anywhere; it finds the metric-collector repo root (four levels above k8s/scripts).
#
# Environment (optional):
#   SKIP_BASE_IMAGE_PULL=1  — do not run docker pull before build (saves time if base is already local).
#   DOCKER_BUILD_PROGRESS=auto|plain|tty  — BuildKit progress (default: plain so layer/Maven output is visible).
#   DOCKER_PLATFORM=linux/arm64|linux/amd64  — pass --platform to pull + build (if pull sits on "Waiting", try linux/arm64 on Apple Silicon).
#
# After building:
#   - OrbStack / Docker Desktop Kubernetes: same image store as Docker — usually no extra step.
#   - kind: kind load docker-image mcac-demo/mcac-init:local
#   - minikube: minikube image load mcac-demo/mcac-init:local
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../../.." && pwd)"
DOCKERFILE="$REPO_ROOT/Dockerfile"
# Must match the FROM in the repo-root Dockerfile (used for a visible docker pull step).
BASE_IMAGE="${MCAC_INIT_BASE_IMAGE:-maven:3.9-eclipse-temurin-8}"

if [[ ! -f "$REPO_ROOT/pom.xml" ]] || [[ ! -f "$DOCKERFILE" ]]; then
  echo "error: expected metric-collector repo at $REPO_ROOT (need pom.xml + Dockerfile)." >&2
  exit 1
fi

export DOCKER_BUILDKIT=1

PLATFORM_ARGS=()
if [[ -n "${DOCKER_PLATFORM:-}" ]]; then
  PLATFORM_ARGS=(--platform "${DOCKER_PLATFORM}")
fi

if [[ "${SKIP_BASE_IMAGE_PULL:-}" != "1" ]]; then
  echo "=== Pulling base image $BASE_IMAGE (layer progress). Skip with SKIP_BASE_IMAGE_PULL=1 ==="
  if [[ ${#PLATFORM_ARGS[@]} -gt 0 ]]; then
    echo "    (with ${PLATFORM_ARGS[*]})"
  fi
  echo "    Note: 'Waiting' on a large layer is normal; first pull can take many minutes."
  echo "    If it never progresses: docker login · try VPN off · DOCKER_PLATFORM=linux/arm64 (Apple Silicon) · restart OrbStack."
  docker pull "${PLATFORM_ARGS[@]}" "$BASE_IMAGE"
else
  echo "=== SKIP_BASE_IMAGE_PULL=1 — using local $BASE_IMAGE if present ==="
fi

echo "=== Building mcac-demo/mcac-init:local (Dockerfile: $DOCKERFILE) ==="
docker build \
  --progress="${DOCKER_BUILD_PROGRESS:-plain}" \
  "${PLATFORM_ARGS[@]}" \
  -t mcac-demo/mcac-init:local \
  -f "$DOCKERFILE" \
  "$REPO_ROOT"

echo "Done. Apply Cassandra manifests, then: kubectl rollout restart statefulset/cassandra -n demo-hub"
