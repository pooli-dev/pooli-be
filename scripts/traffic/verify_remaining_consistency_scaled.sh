#!/usr/bin/env bash
set -euo pipefail

# -----------------------------------------------------------------------------
# Scaled traffic consistency verifier (line_count = 100 / 1000 ...)
# -----------------------------------------------------------------------------
# Verifies all of the following for line_id 1~LINE_COUNT:
# 1) Remaining balance consistency
#    - LINE.remaining_data + Redis remaining_indiv_amount.amount
#    - FAMILY.pool_remaining_data + Redis remaining_shared_amount.amount
# 2) Outbox/fallback table status consistency (MySQL)
#    - TRAFFIC_REDIS_OUTBOX has no non-SUCCESS status
#    - TRAFFIC_REDIS_USAGE_DELTA has no residual rows in test scope
# 3) Mongo done-log consistency
#    - traffic_deduct_done_log count == deterministic expected request count
#    - final_status=FAILED count == 0
#    - per-line req_cnt and deducted_sum stay within group-expected bounds
#
# Assumptions:
# - 1MB = 1,048,576 bytes
# - Timezone = Asia/Seoul
# - line_id starts at 1 and family_id starts at 1
# - 4 lines per family
# -----------------------------------------------------------------------------

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
ENV_FILE="${ENV_FILE:-$ROOT_DIR/.env}"
LINE_COUNT="${LINE_COUNT:-${1:-}}"

if [[ -z "$LINE_COUNT" ]]; then
  echo "Usage: LINE_COUNT=<count> $0"
  echo "   or: $0 <count>"
  exit 1
fi

if ! [[ "$LINE_COUNT" =~ ^[0-9]+$ ]]; then
  echo "ERROR: LINE_COUNT must be a positive integer. got=$LINE_COUNT"
  exit 1
fi

if (( LINE_COUNT <= 0 || LINE_COUNT % 4 != 0 )); then
  echo "ERROR: LINE_COUNT must be > 0 and a multiple of 4. got=$LINE_COUNT"
  exit 1
fi

if [[ ! -f "$ENV_FILE" ]]; then
  echo "ERROR: env file not found: $ENV_FILE"
  exit 1
fi

# Load .env as plain KEY=VALUE pairs without shell evaluation.
# This avoids parsing issues for values containing '&', '?', '#', etc.
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

if ! command -v mysql >/dev/null 2>&1; then
  echo "ERROR: mysql client is required."
  exit 1
fi

if ! command -v redis-cli >/dev/null 2>&1; then
  echo "ERROR: redis-cli is required."
  exit 1
fi

if ! command -v mongosh >/dev/null 2>&1; then
  echo "ERROR: mongosh is required for done-log verification."
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

MONGO_URI_EFFECTIVE="${LOCAL_MONGO_URI:-${MONGO_URI:-}}"
MONGO_DB_NAME="${MONGO_DB_NAME:-pooli}"

ONE_MB=1048576
LINE_FULL_BYTES=$((100 * ONE_MB))
FAMILY_SHARED_BASE=$((50 * ONE_MB))
# speed:2는 Kbps 원천값을 bytes로 환산한 값이며, 본 도메인에서는
# "시간 버킷"이 아니라 "요청 1건당 최대 처리 바이트" 상한으로 적용된다.
DEFAULT_SPEED_LIMIT_BYTES_PER_REQUEST=125000
G5_APP_SPEED_FIELD="speed:2"
TOTAL_ATTEMPT_MB_PER_LINE=50
TOTAL_ATTEMPT_BYTES=$((TOTAL_ATTEMPT_MB_PER_LINE * ONE_MB))

FAMILY_COUNT=$((LINE_COUNT / 4))

G1_COUNT=$((LINE_COUNT * 25 / 100))
G2_COUNT=$((LINE_COUNT * 25 / 100))
G3_COUNT=$((LINE_COUNT * 25 / 100))
G4_COUNT=$((LINE_COUNT * 125 / 1000))
G5_COUNT=$((LINE_COUNT - G1_COUNT - G2_COUNT - G3_COUNT - G4_COUNT))

G1_START=1
G1_END=$((G1_START + G1_COUNT - 1))
G2_START=$((G1_END + 1))
G2_END=$((G2_START + G2_COUNT - 1))
G3_START=$((G2_END + 1))
G3_END=$((G3_START + G3_COUNT - 1))
G4_START=$((G3_END + 1))
G4_END=$((G4_START + G4_COUNT - 1))
G5_START=$((G4_END + 1))
G5_END=$LINE_COUNT

mysql_query() {
  local sql="$1"
  MYSQL_PWD="$DB_PASSWORD" mysql \
    --batch --skip-column-names --raw \
    -h "$DB_HOST" -P "$DB_PORT" -u "$DB_USERNAME" "$DB_NAME" \
    -e "$sql"
}

mysql_scalar() {
  local sql="$1"
  mysql_query "$sql" | head -n 1
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

mongo_eval() {
  local js="$1"
  if [[ -n "$MONGO_URI_EFFECTIVE" ]]; then
    mongosh "$MONGO_URI_EFFECTIVE" --quiet --eval "$js"
  else
    mongosh --quiet --eval "$js"
  fi
}

# Deterministic chunk count implementation that mirrors k6_scaled_traffic_lib.js.
# This count is used for:
# - expected done-log request count per line
# - G5 expected deduct bound under per-request cap policy
calc_request_count_for_line() {
  local line_id="$1"
  local state remaining req next_chunk

  state=$(( (line_id * 1103515245 + 12345) & 0xFFFFFFFF ))
  remaining=$TOTAL_ATTEMPT_MB_PER_LINE
  req=0

  while (( remaining > 0 )); do
    if (( remaining <= 3 )); then
      req=$((req + 1))
      remaining=0
      continue
    fi

    state=$(( (1664525 * state + 1013904223) & 0xFFFFFFFF ))
    next_chunk=$(( (state * 3) / 4294967296 + 1 ))

    req=$((req + 1))
    remaining=$((remaining - next_chunk))
  done

  echo "$req"
}

# Deterministic chunk simulation that mirrors k6_scaled_traffic_lib.js and
# applies "per request cap" policy for G5.
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
      # 음수 speed cap은 제한 없음으로 간주합니다.
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

line_group_name() {
  local line_id="$1"
  if (( line_id <= G1_END )); then
    echo "G1_NO_RESTRICTION"
    return
  fi
  if (( line_id <= G2_END )); then
    echo "G2_DAILY_20MB"
    return
  fi
  if (( line_id <= G3_END )); then
    echo "G3_APP2_DAILY_5MB"
    return
  fi
  if (( line_id <= G4_END )); then
    echo "G4_SHARED_ONLY"
    return
  fi
  echo "G5_APP2_SPEED_1MBPS"
}

family_has_g4_line() {
  local family_id="$1"
  local line_start line_end
  line_start=$(( (family_id - 1) * 4 + 1 ))
  line_end=$(( line_start + 3 ))

  if (( line_end < G4_START || line_start > G4_END )); then
    echo 0
  else
    echo 1
  fi
}

print_header() {
  echo "==============================================="
  echo "Scaled Remaining Consistency Verification"
  echo "time_zone               : Asia/Seoul"
  echo "target_month(yyyymm)    : $CURRENT_YYYYMM"
  echo "line_count              : $LINE_COUNT"
  echo "family_count            : $FAMILY_COUNT"
  echo "groups                  : g1=$G1_COUNT g2=$G2_COUNT g3=$G3_COUNT g4=$G4_COUNT g5=$G5_COUNT"
  echo "mysql                   : $DB_HOST:$DB_PORT/$DB_NAME"
  echo "redis(cache)            : $CACHE_REDIS_HOST:$CACHE_REDIS_PORT"
  echo "redis_namespace         : $REDIS_NAMESPACE"
  echo "mongo_db                : $MONGO_DB_NAME"
  echo "==============================================="
}

fail_count=0

declare -a EXPECTED_REQ_COUNT
declare -a G5_EXPECTED_DEDUCT
declare -a G5_EFFECTIVE_SPEED_LIMIT
TOTAL_EXPECTED_REQUESTS=0

precompute_expectations() {
  local line_id req_count group_name expected_deduct
  local speed_key raw_speed effective_speed

  for line_id in $(seq 1 "$LINE_COUNT"); do
    req_count="$(calc_request_count_for_line "$line_id")"
    EXPECTED_REQ_COUNT[$line_id]="$req_count"
    TOTAL_EXPECTED_REQUESTS=$((TOTAL_EXPECTED_REQUESTS + req_count))

    group_name="$(line_group_name "$line_id")"
    if [[ "$group_name" == "G5_APP2_SPEED_1MBPS" ]]; then
      speed_key="${REDIS_NAMESPACE}:app_speed_limit:${line_id}"
      raw_speed="$(redis_hget_field "$speed_key" "$G5_APP_SPEED_FIELD")"
      effective_speed=$DEFAULT_SPEED_LIMIT_BYTES_PER_REQUEST
      # G5 상한은 고정 상수 대신 런타임 app_speed_limit 해시값(있으면)을 우선 사용한다.
      # 키가 비어 있거나 비정상인 경우에만 기본값(요청당 125000B)으로 안전하게 fallback한다.
      if [[ "$raw_speed" =~ ^[0-9]+$ ]] && (( raw_speed > 0 )); then
        effective_speed=$raw_speed
      fi
      G5_EFFECTIVE_SPEED_LIMIT[$line_id]="$effective_speed"

      expected_deduct="$(calc_g5_expected_deduct_for_line "$line_id" "$effective_speed")"
      G5_EXPECTED_DEDUCT[$line_id]="$expected_deduct"
    else
      G5_EXPECTED_DEDUCT[$line_id]=0
      G5_EFFECTIVE_SPEED_LIMIT[$line_id]=0
    fi
  done
}

verify_line_remaining() {
  echo
  echo "[1] Line Remaining (MySQL + Redis hash.amount)"
  printf "%-6s %-14s %-14s %-14s %-16s %-8s\n" \
    "line" "mysql" "redis" "sum_total" "expected" "result"

  local line_id mysql_value redis_key redis_value total_value expected_text
  local lower upper group_name g5_expected_deduct

  for line_id in $(seq 1 "$LINE_COUNT"); do
    mysql_value="$(mysql_scalar "SELECT COALESCE(remaining_data,0) FROM LINE WHERE line_id=${line_id} AND deleted_at IS NULL LIMIT 1;")"
    mysql_value="${mysql_value:-0}"

    redis_key="${REDIS_NAMESPACE}:remaining_indiv_amount:${line_id}:${CURRENT_YYYYMM}"
    redis_value="$(redis_hget_amount "$redis_key")"
    total_value=$((mysql_value + redis_value))

    group_name="$(line_group_name "$line_id")"

    if [[ "$group_name" == "G1_NO_RESTRICTION" ]]; then
      lower=$((50 * ONE_MB))
      upper=$lower
      expected_text="$lower"
    elif [[ "$group_name" == "G2_DAILY_20MB" ]]; then
      lower=$((80 * ONE_MB))
      upper=$lower
      expected_text="$lower"
    elif [[ "$group_name" == "G3_APP2_DAILY_5MB" ]]; then
      lower=$((95 * ONE_MB))
      upper=$lower
      expected_text="$lower"
    elif [[ "$group_name" == "G4_SHARED_ONLY" ]]; then
      lower=0
      upper=0
      expected_text="0"
    else
      g5_expected_deduct="${G5_EXPECTED_DEDUCT[$line_id]:-0}"
      lower=$((LINE_FULL_BYTES - g5_expected_deduct))
      upper=$lower
      expected_text="$lower"
    fi

    if (( total_value >= lower && total_value <= upper )); then
      printf "%-6s %-14s %-14s %-14s %-16s %-8s\n" \
        "$line_id" "$mysql_value" "$redis_value" "$total_value" "$expected_text" "PASS"
    else
      printf "%-6s %-14s %-14s %-14s %-16s %-8s\n" \
        "$line_id" "$mysql_value" "$redis_value" "$total_value" "$expected_text" "FAIL"
      fail_count=$((fail_count + 1))
    fi
  done
}

verify_family_shared_remaining() {
  echo
  echo "[2] Family Shared Remaining (MySQL + Redis hash.amount)"
  printf "%-8s %-14s %-14s %-14s %-14s %-8s\n" \
    "family" "mysql" "redis" "sum_total" "expected" "result"

  local family_id mysql_value redis_key redis_value total_value expected_value has_g4

  for family_id in $(seq 1 "$FAMILY_COUNT"); do
    mysql_value="$(mysql_scalar "SELECT COALESCE(pool_remaining_data,0) FROM FAMILY WHERE family_id=${family_id} AND deleted_at IS NULL LIMIT 1;")"
    mysql_value="${mysql_value:-0}"

    redis_key="${REDIS_NAMESPACE}:remaining_shared_amount:${family_id}:${CURRENT_YYYYMM}"
    redis_value="$(redis_hget_amount "$redis_key")"
    total_value=$((mysql_value + redis_value))

    has_g4="$(family_has_g4_line "$family_id")"
    if (( has_g4 == 1 )); then
      expected_value=0
    else
      expected_value=$FAMILY_SHARED_BASE
    fi

    if (( total_value == expected_value )); then
      printf "%-8s %-14s %-14s %-14s %-14s %-8s\n" \
        "$family_id" "$mysql_value" "$redis_value" "$total_value" "$expected_value" "PASS"
    else
      printf "%-8s %-14s %-14s %-14s %-14s %-8s\n" \
        "$family_id" "$mysql_value" "$redis_value" "$total_value" "$expected_value" "FAIL"
      fail_count=$((fail_count + 1))
    fi
  done
}

verify_mysql_fallback_tables() {
  local outbox_non_success usage_delta_count

  echo
  echo "[3] MySQL Outbox/Fallback Tables"

  echo "- TRAFFIC_REDIS_OUTBOX status distribution"
  mysql_query "SELECT status, COUNT(*) FROM TRAFFIC_REDIS_OUTBOX GROUP BY status ORDER BY status;" || true

  outbox_non_success="$(mysql_scalar "SELECT COALESCE(SUM(CASE WHEN status <> 'SUCCESS' THEN 1 ELSE 0 END),0) FROM TRAFFIC_REDIS_OUTBOX;")"
  outbox_non_success="${outbox_non_success:-0}"

  if (( outbox_non_success == 0 )); then
    echo "  result: PASS (non-success outbox rows = 0)"
  else
    echo "  result: FAIL (non-success outbox rows = $outbox_non_success)"
    fail_count=$((fail_count + 1))
  fi

  echo "- TRAFFIC_REDIS_USAGE_DELTA status distribution (line 1~$LINE_COUNT)"
  mysql_query "SELECT status, COUNT(*) FROM TRAFFIC_REDIS_USAGE_DELTA WHERE line_id BETWEEN 1 AND $LINE_COUNT GROUP BY status ORDER BY status;" || true

  usage_delta_count="$(mysql_scalar "SELECT COUNT(*) FROM TRAFFIC_REDIS_USAGE_DELTA WHERE line_id BETWEEN 1 AND $LINE_COUNT;")"
  usage_delta_count="${usage_delta_count:-0}"

  if (( usage_delta_count == 0 )); then
    echo "  result: PASS (usage-delta rows in scope = 0)"
  else
    echo "  result: FAIL (usage-delta rows in scope = $usage_delta_count)"
    fail_count=$((fail_count + 1))
  fi
}

verify_mongo_done_log() {
  local total_count failed_count_mongo
  local line_id expected_req actual_req actual_deduct group_name lower upper family_id
  local family_deduct expected_family g4_line_count potential_deduct

  echo
  echo "[4] Mongo Done-Log (traffic_deduct_done_log)"

  total_count="$(mongo_eval "const d=db.getSiblingDB('${MONGO_DB_NAME}'); print(d.traffic_deduct_done_log.countDocuments({ line_id: { \$gte: 1, \$lte: ${LINE_COUNT} } }));")"
  failed_count_mongo="$(mongo_eval "const d=db.getSiblingDB('${MONGO_DB_NAME}'); print(d.traffic_deduct_done_log.countDocuments({ line_id: { \$gte: 1, \$lte: ${LINE_COUNT} }, final_status: 'FAILED' }));")"

  total_count="${total_count//[[:space:]]/}"
  failed_count_mongo="${failed_count_mongo//[[:space:]]/}"

  echo "- done-log total_count   : $total_count (expected=$TOTAL_EXPECTED_REQUESTS)"
  echo "- done-log failed_count  : $failed_count_mongo (expected=0)"

  if [[ "$total_count" != "$TOTAL_EXPECTED_REQUESTS" ]]; then
    echo "  result: FAIL (done-log count mismatch)"
    fail_count=$((fail_count + 1))
  else
    echo "  result: PASS (done-log count matched)"
  fi

  if [[ "$failed_count_mongo" != "0" ]]; then
    echo "  result: FAIL (FAILED done-log exists)"
    fail_count=$((fail_count + 1))
  else
    echo "  result: PASS (no FAILED done-log)"
  fi

  declare -a MONGO_REQ_CNT
  declare -a MONGO_DEDUCTED_SUM
  declare -a FAMILY_G4_DEDUCTED_SUM
  declare -a FAMILY_G4_LINE_COUNT

  # Mongo 숫자 타입(Long/Int)을 안전하게 bash에서 비교하기 위해 Number()로 강제 출력한다.
  while IFS=$'\t' read -r agg_line_id agg_req_cnt agg_deducted_sum; do
    [[ -z "${agg_line_id:-}" ]] && continue
    MONGO_REQ_CNT[$agg_line_id]="${agg_req_cnt:-0}"
    MONGO_DEDUCTED_SUM[$agg_line_id]="${agg_deducted_sum:-0}"
  done < <(
    mongo_eval "const d=db.getSiblingDB('${MONGO_DB_NAME}'); d.traffic_deduct_done_log.aggregate([{ \$match: { line_id: { \$gte: 1, \$lte: ${LINE_COUNT} } } }, { \$group: { _id: '\$line_id', req_cnt: { \$sum: 1 }, deducted_sum: { \$sum: '\$deducted_total_bytes' } } }, { \$sort: { _id: 1 } }]).forEach(doc => print(Number(doc._id) + '\\t' + Number(doc.req_cnt) + '\\t' + Number(doc.deducted_sum)));"
  )

  echo "- per-line req_cnt/deducted_sum"
  printf "%-6s %-10s %-10s %-14s %-16s %-8s\n" \
    "line" "req_exp" "req_act" "deducted_sum" "expected" "result"

  for line_id in $(seq 1 "$LINE_COUNT"); do
    expected_req="${EXPECTED_REQ_COUNT[$line_id]:-0}"
    actual_req="${MONGO_REQ_CNT[$line_id]:-0}"
    actual_deduct="${MONGO_DEDUCTED_SUM[$line_id]:-0}"
    group_name="$(line_group_name "$line_id")"
    family_id=$(( (line_id - 1) / 4 + 1 ))

    if [[ "$group_name" == "G1_NO_RESTRICTION" ]]; then
      lower=$TOTAL_ATTEMPT_BYTES
      upper=$TOTAL_ATTEMPT_BYTES
    elif [[ "$group_name" == "G2_DAILY_20MB" ]]; then
      lower=$((20 * ONE_MB))
      upper=$((20 * ONE_MB))
    elif [[ "$group_name" == "G3_APP2_DAILY_5MB" ]]; then
      lower=$((5 * ONE_MB))
      upper=$((5 * ONE_MB))
    elif [[ "$group_name" == "G4_SHARED_ONLY" ]]; then
      lower=0
      upper=$TOTAL_ATTEMPT_BYTES
      # G4는 개인별 차감량 분배가 비결정적이므로 라인 단위 금액 검증을 하지 않고,
      # 아래의 가족 단위 합산 검증으로 성공/실패를 판정한다.
      FAMILY_G4_DEDUCTED_SUM[$family_id]=$(( ${FAMILY_G4_DEDUCTED_SUM[$family_id]:-0} + actual_deduct ))
      FAMILY_G4_LINE_COUNT[$family_id]=$(( ${FAMILY_G4_LINE_COUNT[$family_id]:-0} + 1 ))
    else
      lower="${G5_EXPECTED_DEDUCT[$line_id]:-0}"
      upper=$lower
    fi

    if [[ "$group_name" == "G4_SHARED_ONLY" ]]; then
      if [[ "$actual_req" == "$expected_req" ]]; then
        printf "%-6s %-10s %-10s %-14s %-16s %-8s\n" \
          "$line_id" "$expected_req" "$actual_req" "$actual_deduct" "family_total_only" "PASS"
      else
        printf "%-6s %-10s %-10s %-14s %-16s %-8s\n" \
          "$line_id" "$expected_req" "$actual_req" "$actual_deduct" "family_total_only" "FAIL"
        fail_count=$((fail_count + 1))
      fi
    else
      if [[ "$actual_req" == "$expected_req" ]] && (( actual_deduct >= lower && actual_deduct <= upper )); then
        if (( lower == upper )); then
          printf "%-6s %-10s %-10s %-14s %-16s %-8s\n" \
            "$line_id" "$expected_req" "$actual_req" "$actual_deduct" "$lower" "PASS"
        else
          printf "%-6s %-10s %-10s %-14s %-16s %-8s\n" \
            "$line_id" "$expected_req" "$actual_req" "$actual_deduct" "[$lower,$upper]" "PASS"
        fi
      else
        if (( lower == upper )); then
          printf "%-6s %-10s %-10s %-14s %-16s %-8s\n" \
            "$line_id" "$expected_req" "$actual_req" "$actual_deduct" "$lower" "FAIL"
        else
          printf "%-6s %-10s %-10s %-14s %-16s %-8s\n" \
            "$line_id" "$expected_req" "$actual_req" "$actual_deduct" "[$lower,$upper]" "FAIL"
        fi
        fail_count=$((fail_count + 1))
      fi
    fi
  done

  echo "- G4 family aggregated deducted_sum"
  printf "%-8s %-10s %-14s %-14s %-8s\n" \
    "family" "g4_lines" "deducted_sum" "expected" "result"

  for family_id in $(seq 1 "$FAMILY_COUNT"); do
    g4_line_count="${FAMILY_G4_LINE_COUNT[$family_id]:-0}"
    if (( g4_line_count == 0 )); then
      continue
    fi

    family_deduct="${FAMILY_G4_DEDUCTED_SUM[$family_id]:-0}"
    potential_deduct=$((g4_line_count * TOTAL_ATTEMPT_BYTES))
    expected_family=$potential_deduct
    if (( expected_family > FAMILY_SHARED_BASE )); then
      expected_family=$FAMILY_SHARED_BASE
    fi

    if (( family_deduct == expected_family )); then
      printf "%-8s %-10s %-14s %-14s %-8s\n" \
        "$family_id" "$g4_line_count" "$family_deduct" "$expected_family" "PASS"
    else
      printf "%-8s %-10s %-14s %-14s %-8s\n" \
        "$family_id" "$g4_line_count" "$family_deduct" "$expected_family" "FAIL"
      fail_count=$((fail_count + 1))
    fi
  done
}

print_summary() {
  echo
  echo "-----------------------------------------------"
  if (( fail_count == 0 )); then
    echo "FINAL RESULT: PASS (all consistency checks matched)"
  else
    echo "FINAL RESULT: FAIL (${fail_count} mismatched checks found)"
  fi
  echo "-----------------------------------------------"
}

print_header
precompute_expectations
verify_line_remaining
verify_family_shared_remaining
verify_mysql_fallback_tables
verify_mongo_done_log
print_summary

if (( fail_count > 0 )); then
  exit 1
fi
