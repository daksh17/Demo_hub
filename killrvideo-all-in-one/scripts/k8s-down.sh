#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

echo "Removing KillrVideo stack from Kubernetes..."
kubectl delete -k k8s/ --ignore-not-found
