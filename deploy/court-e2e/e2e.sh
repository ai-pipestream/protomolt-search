#!/usr/bin/env bash
# One-command end-to-end run: build, bring the stack up, gate on the
# pipeline's verify step, tear down. CI-friendly: the script's exit code
# is the pipeline's exit code (0 = PASS), and all containers are removed
# afterwards. Volumes are KEPT by default (seed/extract/chunk stages
# resume); pass --clean to wipe them first.
#
#   deploy/court-e2e/e2e.sh [--clean]
set -euo pipefail
cd "$(dirname "$0")"

[ -f .env ] || cp .env.example .env

if [ "${1:-}" = "--clean" ]; then
  docker compose --env-file .env down -v
fi

docker compose --env-file .env build
docker compose --env-file .env up -d

rc=0
docker wait "$(docker compose ps -q pipeline)" >/dev/null 2>&1 || true
rc=$(docker inspect --format '{{.State.ExitCode}}' "$(docker compose ps -q pipeline)")

echo "================ pipeline log (tail) ================"
docker compose logs --no-log-prefix pipeline | tail -15
echo "====================================================="

if [ "$rc" = "0" ]; then
  echo "E2E PASS (coordinator still up on :${COORD_PORT:-50050}; 'docker compose down' to stop)"
else
  echo "E2E FAIL (exit $rc)"
  docker compose --env-file .env down
fi
exit "$rc"
