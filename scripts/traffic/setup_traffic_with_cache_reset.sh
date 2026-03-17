#!/usr/bin/env bash
set -euo pipefail

# -----------------------------------------------------------------------------
# Traffic setup helper
# -----------------------------------------------------------------------------
# This helper makes setup deterministic by:
# 1) deleting all Redis keys under REDIS_NAMESPACE (cache Redis)
# 2) applying the requested SQL setup script to MySQL
#
# Usage:
#   ./setup_traffic_with_cache_reset.sh <setup_sql_file>
# -----------------------------------------------------------------------------

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
ENV_FILE="${ENV_FILE:-$ROOT_DIR/.env}"
SETUP_SQL_FILE="${1:-}"

if [[ -z "$SETUP_SQL_FILE" ]]; then
  echo "ERROR: setup sql file path is required."
  echo "usage: $0 <setup_sql_file>"
  exit 1
fi

if [[ ! -f "$SETUP_SQL_FILE" ]]; then
  echo "ERROR: setup sql file not found: $SETUP_SQL_FILE"
  exit 1
fi

if [[ ! -f "$ENV_FILE" ]]; then
  echo "ERROR: env file not found: $ENV_FILE"
  exit 1
fi

if ! command -v mysql >/dev/null 2>&1; then
  echo "ERROR: mysql client is required."
  exit 1
fi

if ! command -v redis-cli >/dev/null 2>&1; then
  echo "ERROR: redis-cli is required."
  exit 1
fi

# Load .env as plain KEY=VALUE pairs without shell evaluation.
load_env_file() {
  local env_path="$1"
  while IFS= read -r line || [[ -n "$line" ]]; do
    [[ -z "${line//[[:space:]]/}" ]] && continue
    [[ "${line:0:1}" == "#" ]] && continue

    if [[ "$line" =~ ^([A-Za-z_][A-Za-z0-9_]*)=(.*)$ ]]; then
      local key="${BASH_REMATCH[1]}"
      local value="${BASH_REMATCH[2]}"
      value="${value%$'\r'}"

      if [[ "$value" =~ ^\"(.*)\"$ ]]; then
        value="${BASH_REMATCH[1]}"
      elif [[ "$value" =~ ^\'(.*)\'$ ]]; then
        value="${BASH_REMATCH[1]}"
      fi

      export "$key=$value"
    fi
  done < "$env_path"
}

load_env_file "$ENV_FILE"

if [[ -z "${DB_URL:-}" || -z "${DB_USERNAME:-}" || -z "${DB_PASSWORD:-}" ]]; then
  echo "ERROR: DB_URL / DB_USERNAME / DB_PASSWORD must be set in $ENV_FILE"
  exit 1
fi

if [[ -z "${CACHE_REDIS_HOST:-}" || -z "${CACHE_REDIS_PORT:-}" ]]; then
  echo "ERROR: CACHE_REDIS_HOST / CACHE_REDIS_PORT must be set in $ENV_FILE"
  exit 1
fi

DB_URL_NO_PREFIX="${DB_URL#jdbc:mysql://}"
DB_HOST_PORT_DB="${DB_URL_NO_PREFIX%%\?*}"
DB_HOST_PORT="${DB_HOST_PORT_DB%%/*}"
DB_NAME="${DB_HOST_PORT_DB#*/}"
DB_HOST="${DB_HOST_PORT%%:*}"
DB_PORT_PART="${DB_HOST_PORT#*:}"
DB_PORT="${DB_PORT_PART:-3306}"
if [[ "$DB_HOST_PORT" == "$DB_PORT_PART" ]]; then
  DB_PORT="3306"
fi

REDIS_NAMESPACE="${REDIS_NAMESPACE:-pooli}"
REDIS_MATCH_PATTERN="${REDIS_NAMESPACE}:*"

redis_del_pattern() {
  local pattern="$1"
  local cursor="0"
  local -a redis_base
  local -a scan_rows
  local -a keys

  redis_base=(
    redis-cli
    -h "$CACHE_REDIS_HOST"
    -p "$CACHE_REDIS_PORT"
    --no-auth-warning
  )
  if [[ -n "${CACHE_REDIS_PASSWORD:-}" ]]; then
    redis_base=(
      redis-cli
      -h "$CACHE_REDIS_HOST"
      -p "$CACHE_REDIS_PORT"
      -a "$CACHE_REDIS_PASSWORD"
      --no-auth-warning
    )
  fi

  while true; do
    mapfile -t scan_rows < <("${redis_base[@]}" --raw SCAN "$cursor" MATCH "$pattern" COUNT 1000)
    cursor="${scan_rows[0]:-0}"

    if (( ${#scan_rows[@]} > 1 )); then
      keys=("${scan_rows[@]:1}")
      # 존재하는 키만 묶어서 DEL하여 setup 전 상태를 깔끔히 초기화합니다.
      "${redis_base[@]}" DEL "${keys[@]}" >/dev/null || true
    fi

    if [[ "$cursor" == "0" ]]; then
      break
    fi
  done
}

echo "==============================================="
echo "Traffic Setup With Cache Reset"
echo "env_file                : $ENV_FILE"
echo "setup_sql               : $SETUP_SQL_FILE"
echo "mysql                   : $DB_HOST:$DB_PORT/$DB_NAME"
echo "redis(cache)            : $CACHE_REDIS_HOST:$CACHE_REDIS_PORT"
echo "redis_namespace         : $REDIS_NAMESPACE"
echo "==============================================="

echo "[1/2] Reset cache redis keys (${REDIS_MATCH_PATTERN})"
redis_del_pattern "$REDIS_MATCH_PATTERN"
echo "  done"

echo "[2/2] Apply setup sql"
MYSQL_PWD="$DB_PASSWORD" mysql \
  --default-character-set=utf8mb4 \
  --batch --raw \
  -h "$DB_HOST" -P "$DB_PORT" -u "$DB_USERNAME" "$DB_NAME" \
  < "$SETUP_SQL_FILE"
echo "  done"

echo
echo "Setup completed."
echo "Next step:"
echo "1) run k6 test"
echo "2) run verify script"
