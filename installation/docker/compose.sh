#!/usr/bin/env bash
# Always run the CoRE Stack Compose project with its explicitly named env file.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
COMPOSE_ENV_FILE="${CORESTACK_COMPOSE_ENV_FILE:-$REPO_ROOT/.env.core-stack-docker}"

if [ ! -f "$COMPOSE_ENV_FILE" ]; then
    echo "ERROR: Compose environment file not found: $COMPOSE_ENV_FILE" >&2
    echo "Create it with:" >&2
    echo "  cp installation/docker/env.core-stack-docker.example .env.core-stack-docker" >&2
    exit 1
fi

# --gpu (first argument) enables the serialized heavy worker and GPU access:
# the "heavy" Compose profile creates celery-heavy with the NVIDIA device, and
# the app is told it may queue long tasks. Without it, no heavy container is
# created and those endpoints answer with a clear error instead of queueing
# work nothing would run. Requires an NVIDIA GPU and the Container Toolkit.
if [ "${1:-}" = "--gpu" ]; then
    shift
    export COMPOSE_PROFILES="${COMPOSE_PROFILES:+$COMPOSE_PROFILES,}heavy"
    export GPU_AVAILABLE="${GPU_AVAILABLE:-True}"
    export HEAVY_WORKER_ENABLED="${HEAVY_WORKER_ENABLED:-True}"
fi

exec docker compose \
    --project-directory "$REPO_ROOT" \
    --env-file "$COMPOSE_ENV_FILE" \
    -f "$REPO_ROOT/docker-compose.yml" \
    "$@"
