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

exec docker compose \
    --project-directory "$REPO_ROOT" \
    --env-file "$COMPOSE_ENV_FILE" \
    -f "$REPO_ROOT/docker-compose.yml" \
    "$@"
