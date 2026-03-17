#!/usr/bin/env bash
set -euo pipefail

# -----------------------------------------------------------------------------
# Small traffic test remaining-balance verifier
# -----------------------------------------------------------------------------
# What this script verifies:
# 1) Line remaining = MySQL LINE.remaining_data + Redis remaining_indiv_amount hash.amount
# 2) Family shared remaining = MySQL FAMILY.pool_remaining_data + Redis remaining_shared_amount hash.amount
# 3) PASS/FAIL against scenario expectations
#
# Assumptions (same as test setup):
# - 1MB = 1,048,576 bytes
# - Timezone = Asia/Seoul
# - line_id 1~16, family_id 1~4
#
# Group expectation rules:
# - G1 (line 1~4):   exact 50MB remaining
# - G2 (line 5~8):   exact 80MB remaining
# - G3 (line 9~12):  exact 95MB remaining
# - G4 (line 13~14): exact 0B remaining (line 개인풀은 0으로 시작)
# - G5 (line 15~16): exact remaining by deterministic request simulation
#   with "per-request cap" policy (no time-bucket formula)
# -----------------------------------------------------------------------------

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
ENV_FILE="${ENV_FILE:-$ROOT_DIR/.env}"

if [[ ! -f "$ENV_FILE" ]]; then
  echo "ERROR: env file not found: $ENV_FILE"
  exit 1
fi

# Load .env as plain KEY=VALUE pairs without shell evaluation.
# This avoids parsing issues for values containing '&', '?', '#', etc.
load_env_file() {
  local env_path="$1"
  while IFS= read -r line || [[ -n "$line" ]]; do
    # Skip comments / blank lines.
    [[ -z "${line//[[:space:]]/}" ]] && continue
    [[ "${line:0:1}" == "#" ]] && continue

    # Keep only KEY=VALUE records.
    if [[ "$line" =~ ^([A-Za-z_][A-Za-z0-9_]*)=(.*)$ ]]; then
      local key="${BASH_REMATCH[1]}"
      local value="${BASH_REMATCH[2]}"

      # Trim trailing CR for CRLF files.
      value="${value%$'\r'}"

      # Remove surrounding single/double quotes if present.
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

if ! command -v mysql >/dev/null 2>&1; then
  echo "ERROR: mysql client is required."
  exit 1
fi

if ! command -v redis-cli >/dev/null 2>&1; then
  echo "ERROR: redis-cli is required."
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
CURRENT_YYYYMM="$(TZ=Asia/Seoul date +%Y%m)"

ONE_MB=1048576
LINE_FULL_BYTES=$((100 * ONE_MB))
FAMILY_SHARED_BASE=52428800  # 50MB
DEFAULT_SPEED_LIMIT_BYTES_PER_REQUEST=125000
G5_APP_SPEED_FIELD="speed:2"
TOTAL_ATTEMPT_MB_PER_LINE=50
TOTAL_ATTEMPT_BYTES=$((TOTAL_ATTEMPT_MB_PER_LINE * ONE_MB))

# Expected remaining values by line.
EXPECTED_LINE_EXACT() {
  local line_id="$1"
  if (( line_id >= 1 && line_id <= 4 )); then
    echo $((50 * ONE_MB))
    return
  fi
  if (( line_id >= 5 && line_id <= 8 )); then
    echo $((80 * ONE_MB))
    return
  fi
  if (( line_id >= 9 && line_id <= 12 )); then
    echo $((95 * ONE_MB))
    return
  fi
  if (( line_id == 13 || line_id == 14 )); then
    echo 0
    return
  fi
  # G5는 런타임 설정 기반으로 별도 계산한다.
  echo -1
}

mysql_scalar() {
  local sql="$1"
  MYSQL_PWD="$DB_PASSWORD" mysql \
    --batch --skip-column-names --raw \
    -h "$DB_HOST" -P "$DB_PORT" -u "$DB_USERNAME" "$DB_NAME" \
    -e "$sql"
}

redis_hget_amount() {
  local key="$1"
  redis_hget_field "$key" "amount"
}

redis_hget_field() {
  local key="$1"
  local field="$2"
  local value
  local -a redis_cmd

  redis_cmd=(
    redis-cli
    -h "$CACHE_REDIS_HOST"
    -p "$CACHE_REDIS_PORT"
    --no-auth-warning
    HGET "$key" "$field"
  )

  if [[ -n "${CACHE_REDIS_PASSWORD:-}" ]]; then
    redis_cmd=(
      redis-cli
      -h "$CACHE_REDIS_HOST"
      -p "$CACHE_REDIS_PORT"
      -a "$CACHE_REDIS_PASSWORD"
      --no-auth-warning
      HGET "$key" "$field"
    )
  fi

  value="$("${redis_cmd[@]}" || true)"
  if [[ -z "${value:-}" || "$value" == "(nil)" ]]; then
    echo 0
    return
  fi
  echo "$value"
}

# k6 스케일드 테스트와 동일한 deterministic 청크 시뮬레이션으로
# G5 라인의 예상 차감량을 계산한다.
calc_g5_expected_deduct_for_line() {
  local line_id="$1"
  local cap_per_request="$2"
  local state remaining next_chunk_mb chunk_bytes per_request_deduct deducted_sum

  state=$(( (line_id * 1103515245 + 12345) & 0xFFFFFFFF ))
  remaining=$TOTAL_ATTEMPT_MB_PER_LINE
  deducted_sum=0

  while (( remaining > 0 )); do
    if (( remaining <= 3 )); then
      next_chunk_mb=$remaining
      remaining=0
    else
      state=$(( (1664525 * state + 1013904223) & 0xFFFFFFFF ))
      next_chunk_mb=$(( (state * 3) / 4294967296 + 1 ))
      remaining=$((remaining - next_chunk_mb))
    fi

    chunk_bytes=$((next_chunk_mb * ONE_MB))
    if (( cap_per_request < 0 )); then
      per_request_deduct=$chunk_bytes
    else
      per_request_deduct=$chunk_bytes
      if (( cap_per_request < per_request_deduct )); then
        per_request_deduct=$cap_per_request
      fi
    fi
    deducted_sum=$((deducted_sum + per_request_deduct))
  done

  if (( deducted_sum > TOTAL_ATTEMPT_BYTES )); then
    deducted_sum=$TOTAL_ATTEMPT_BYTES
  fi
  echo "$deducted_sum"
}

declare -a G5_EXPECTED_REMAINING

precompute_g5_expected_remaining() {
  local line_id speed_key raw_speed effective_speed expected_deduct expected_remaining
  for line_id in 15 16; do
    speed_key="${REDIS_NAMESPACE}:app_speed_limit:${line_id}"
    raw_speed="$(redis_hget_field "$speed_key" "$G5_APP_SPEED_FIELD")"
    effective_speed=$DEFAULT_SPEED_LIMIT_BYTES_PER_REQUEST
    if [[ "$raw_speed" =~ ^[0-9]+$ ]] && (( raw_speed > 0 )); then
      effective_speed=$raw_speed
    fi

    expected_deduct="$(calc_g5_expected_deduct_for_line "$line_id" "$effective_speed")"
    expected_remaining=$((LINE_FULL_BYTES - expected_deduct))
    if (( expected_remaining < 0 )); then
      expected_remaining=0
    fi
    G5_EXPECTED_REMAINING[$line_id]="$expected_remaining"
  done
}

print_header() {
  echo "==============================================="
  echo "Remaining Consistency Verification"
  echo "time_zone               : Asia/Seoul"
  echo "target_month(yyyymm)    : $CURRENT_YYYYMM"
  echo "mysql                   : $DB_HOST:$DB_PORT/$DB_NAME"
  echo "redis(cache)            : $CACHE_REDIS_HOST:$CACHE_REDIS_PORT"
  echo "redis_namespace         : $REDIS_NAMESPACE"
  echo "==============================================="
}

fail_count=0

verify_line_remaining() {
  echo
  echo "[1] Line Remaining (MySQL + Redis hash.amount)"
  printf "%-6s %-14s %-14s %-14s %-16s %-8s\n" \
    "line" "mysql" "redis" "sum_total" "expected" "result"

  for line_id in $(seq 1 16); do
    local mysql_value redis_key redis_value total_value expected_exact result_text
    mysql_value="$(mysql_scalar "SELECT COALESCE(remaining_data,0) FROM LINE WHERE line_id=${line_id} AND deleted_at IS NULL LIMIT 1;")"
    mysql_value="${mysql_value:-0}"
    redis_key="${REDIS_NAMESPACE}:remaining_indiv_amount:${line_id}:${CURRENT_YYYYMM}"
    redis_value="$(redis_hget_amount "$redis_key")"
    total_value=$((mysql_value + redis_value))
    expected_exact="$(EXPECTED_LINE_EXACT "$line_id")"

    if (( line_id >= 15 && line_id <= 16 )); then
      expected_exact="${G5_EXPECTED_REMAINING[$line_id]:-0}"
    fi

    if (( total_value == expected_exact )); then
      result_text="PASS"
    else
      result_text="FAIL"
      fail_count=$((fail_count + 1))
    fi

    printf "%-6s %-14s %-14s %-14s %-16s %-8s\n" \
      "$line_id" "$mysql_value" "$redis_value" "$total_value" "$expected_exact" "$result_text"
  done
}

verify_family_shared_remaining() {
  echo
  echo "[2] Family Shared Remaining (MySQL + Redis hash.amount)"
  printf "%-8s %-14s %-14s %-14s %-14s %-8s\n" \
    "family" "mysql" "redis" "sum_total" "expected" "result"

  for family_id in $(seq 1 4); do
    local mysql_value redis_key redis_value total_value expected_value result_text
    mysql_value="$(mysql_scalar "SELECT COALESCE(pool_remaining_data,0) FROM FAMILY WHERE family_id=${family_id} AND deleted_at IS NULL LIMIT 1;")"
    mysql_value="${mysql_value:-0}"
    redis_key="${REDIS_NAMESPACE}:remaining_shared_amount:${family_id}:${CURRENT_YYYYMM}"
    redis_value="$(redis_hget_amount "$redis_key")"
    total_value=$((mysql_value + redis_value))

    if (( family_id >= 1 && family_id <= 3 )); then
      expected_value="$FAMILY_SHARED_BASE"
    else
      expected_value=0
    fi

    if (( total_value == expected_value )); then
      result_text="PASS"
    else
      result_text="FAIL"
      fail_count=$((fail_count + 1))
    fi

    printf "%-8s %-14s %-14s %-14s %-14s %-8s\n" \
      "$family_id" "$mysql_value" "$redis_value" "$total_value" "$expected_value" "$result_text"
  done
}

print_summary() {
  echo
  echo "-----------------------------------------------"
  if (( fail_count == 0 )); then
    echo "FINAL RESULT: PASS (all remaining-balance checks matched)"
  else
    echo "FINAL RESULT: FAIL (${fail_count} mismatched checks found)"
  fi
  echo "-----------------------------------------------"
}

print_header
precompute_g5_expected_remaining
verify_line_remaining
verify_family_shared_remaining
print_summary

if (( fail_count > 0 )); then
  exit 1
fi
