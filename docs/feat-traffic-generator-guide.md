# **데이터 차감 플로우 최종 명세서 (Final v1.0)**

## **1) 목적**

- 범위:
- 요청 이벤트 단위 처리 방식
- Redis 데이터 모델/TTL 기준
- Lua 스크립트 종류/반환값/호출 순서
- 트래픽 처리 서버 오케스트레이션
- 실패/재시도/로그/중복 처리 규칙

## **2) 용어**

*델타: 최근 10초 간 발생한 평균 데이터 사용량(리필 계산용)*

- `api total data`: API 요청 이벤트 1건에서 처리할 데이터량(Byte).
- `api remaining data`: API 요청 이벤트 1건에서 아직 처리되지 않은 데이터량(Byte).
- `residual data`: 개인풀 처리 후 남은 미처리 데이터량(Byte).
- `traceId`: API 서버가 생성하여 MQ 메시지에 넣는 UUID.
- `refill unit`: 리필 단위(델타 * 10, 0이라면 api total data).
- `redis threshold`: 리필 임계치(델타 * 3, 0이라면 api total data의 30%).
- `db remaining`: DB 원천 잔량(`LINE.remaining_data` 또는 `FAMILY.pool_remaining_data`).
- `actual refill amount`: 실제 리필량. `min(refill unit, db remaining)`으로 계산한다.
- `refill ledger`: DB 차감과 Redis 충전 정합성/재처리를 위한 영속 로그.

## **3) 아키텍처**

API 서버:

- 요청별 traceId 생성
- MQ 적재 성공 시 200
- MQ 적재 실패 시 API 서버 HTTP 응답 코드는 추후 확정(TBD)

트래픽 처리 서버:

- MQ 메시지 소비
- 이벤트 단일 사이클 차감(개인풀 우선, 필요 시 공유풀 보완)
- 최종 결과 영속 저장 후 XACK

저장소:

- Redis: 실시간 정책/카운터/in-flight
- 영속 저장소: 완료 이력(DONE), 최종 결과
- MongoDB 연동 전 임시 방안: MySQL trace_id UNIQUE

## **4) 입출력 계약**

### **4.1 MQ 메시지 필수 필드**

- traceId (string, uuid)
- lineId (long)
- familyId (long)
- appId (int)
- apiTotalData (long, Byte)
- enqueuedAt (epoch millis)

### **4.2 처리 결과 필드**

- traceId
- apiTotalData
- deductedTotalBytes
- apiRemainingData
- finalStatus (SUCCESS | PARTIAL_SUCCESS | FAILED)
- lastLuaStatus
- createdAt, finishedAt

## **5) Redis 키 구조**

### **5.1 전역 정책 종류 활성화**

- policy:{policyId} (string, ttl 없음)

### **5.2 제한/사용량**

- daily_total_limit:{lineId} (string, ttl 없음)
- app_data_daily_limit:{lineId} (hash, ttl 없음) -> limit:{appId}
- app_speed_limit:{lineId} (hash, ttl 없음) -> speed:{appId}
- app_whitelist:{lineId} (set, ttl 없음) -> {appId}
- monthly_shared_limit:{lineId} (string, ttl 없음)
- daily_total_usage:{lineId}:{yyyymmdd} (string, ttl 일말+8h)
- daily_app_usage:{lineId}:{yyyymmdd} (hash, ttl 일말+8h) -> app:{appId}
- monthly_shared_usage:{lineId}:{yyyymm} (string, ttl 월말+10d)

### **5.3 차단**

- immediately_block_end:{lineId} (string, ttl 없음)
- repeat_block:{lineId} (hash, ttl 없음)
- field: day:{day_num}:{repeat_block_id}
- value: "{start_at_sec}:{end_at_sec}"
- day 매핑: 0=SUN ... 6=SAT
- 구간 경계: [start_sec, end_sec] (양끝 포함)

### **5.4 잔량/리필/속도 누적**

- remaining_indiv_amount:{lineId}:{yyyymm} (hash, ttl 월말+10d)
    - amount
    - is_empty (DB 원천 잔량 고갈 플래그, `1`=DB remaining<=0, `0`=그 외)
- remaining_shared_amount:{familyId}:{yyyymm} (hash, ttl 월말+10d)
    - amount
    - is_empty (DB 원천 잔량 고갈 플래그, `1`=DB remaining<=0, `0`=그 외)
- indiv_refill_lock:{lineId} (string, ttl 3000ms, value=traceId)
- shared_refill_lock:{familyId} (string, ttl 3000ms, value=traceId)
- qos:{lineId} (string, ttl 없음)
- speed_bucket:individual:{lineId}:{epochSec} (string, ttl 15초)
- speed_bucket:shared:{familyId}:{epochSec} (string, ttl 15초)
    - value: 해당 초의 누적 차감량(Byte)

### 5.4.1 최근 데이터 사용량 1초 단위 기록

1. 버킷 기록 시점:
    1. 매 tick 처리마다 `actual answer(Byte)`를 기록한다.
    2. 단, `answer <= 0`인 경우는 기록하지 않는다.
2. 버킷 스코프:
    1. 개인풀은 `speed_bucket:individual:{lineId}:{epochSec}`에 기록한다.
    2. 공유풀은 `speed_bucket:shared:{familyId}:{epochSec}`에 기록한다.
3. 기록 방식:
    1. 같은 초(`epochSec`) 내 중복/동시 tick은 누적 증가(`INCRBY`)로 합산한다.
    2. 기록 성공 시마다 해당 키 TTL을 15초로 갱신한다.

### **5.5 in-flight dedupe(중복 처리 방지)**

- dedupe:run:{traceId} (string, ttl 60초)

## **6) 시간/TTL/타임존 규칙**

- 타임존: Asia/Seoul 고정
- 일별 키: EXPIREAT(일말 + 8시간)
- 월별 키: EXPIREAT(월말 + 10일)
- lock TTL: 3000ms
- lock heartbeat: 소유자만 1초 주기로 TTL 연장
- DB refill 수행 중에도 lock 소유자는 `LOCK_HEARTBEAT_MS(1000ms)` 주기로 heartbeat를 유지한다.

상수 고정:

- INFLIGHT_TTL_SEC = 60
- APP_SPEED_USED_TTL_SEC = 3
- LOCK_TTL_MS = 3000
- LOCK_HEARTBEAT_MS = 1000
- HYDRATE_RETRY_MAX = 1

## **7) Lua 반환 계약**

- 모든 차감 Lua는 JSON 문자열(`{"answer": <number>, "status": "<CODE>"}`)을 반환한다.
- answer: 현재 tick 실제 차감량(Byte)
- status:
- 상태 코드 규칙:
  - 정책에 의한 제한/차단(`BLOCKED_*`, `HIT_*`)이 발생하면 `status`는 반드시 해당 정책 상태여야 하며 `OK`를 반환하면 안 된다.
  - `last_lua_status`는 차감 Lua가 반환한 `status`를 그대로 저장한다.

| **코드** | **의미** | **설명** |
| --- | --- | --- |
| **OK** | 정상 처리 | 요청이 정상적으로 처리되었으며, 사용량도 허용 범위 내에 있음 |
| **NO_BALANCE** | 잔량 부족 | 현재 풀(개인 풀 또는 공유 풀)의 잔량이 부족한 상태. 단, `is_empty != 1`일 때만 **리필(refill)** 수행 |
| **BLOCKED_IMMEDIATE** | 즉시 차단 | 즉시 차단 정책이 적용된 상태로 요청이 차단됨 |
| **BLOCKED_REPEAT** | 반복 차단 | 반복 차단 정책의 시간대에 해당하여 요청이 차단됨 |
| **HIT_DAILY_LIMIT** | 일 사용량 제한 도달 | 일일 총 사용량 제한에 도달하여 더 이상 요청 처리 불가 |
| **HIT_MONTHLY_SHARED_LIMIT** | 월 공유풀 제한 도달 | 공유 풀의 월 사용량 제한에 도달하여 요청 처리 불가 |
| **HIT_APP_DAILY_LIMIT** | 앱 일 사용량 제한 도달 | 특정 앱 기준의 일 사용량 제한에 도달 |
| **HIT_APP_SPEED** | 앱 속도 제한 | 앱 속도 제한 정책에 의해 허용량까지만 처리되고 나머지는 제한됨 |
| **HYDRATE** | Redis hydrate 필요 | Redis key가 존재하지 않아 DB에서 데이터를 조회하여 Redis에 hydrate 필요 |
| **ERROR** | 오류 발생 | 입력값 오류, 데이터 오류, 또는 시스템 내부 오류 발생 |

## **8) Lua 스크립트 목록**

| **스크립트** | **목적** | **주요 입력** | **주요 읽기/쓰기** | **반환** |
| --- | --- | --- | --- | --- |
| deduct_indiv_tick.lua | 개인풀 요청량 차감 + 정책 검증 + usage 갱신 | lineId, appId, requestedData, nowSec, traceId | 개인풀 잔량, 차단/제한, 일사용량, 앱사용량, 속도누적 | JSON 문자열(`{"answer":n,"status":"..."}`) |
| deduct_shared_tick.lua | 공유풀 요청량 차감 + 정책 검증 + usage 갱신 | lineId, familyId, appId, requestedData, nowSec, traceId | 공유풀 잔량, 차단/제한, 일/월사용량, 앱사용량, 속도누적 | JSON 문자열(`{"answer":n,"status":"..."}`) |
| refill_gate.lua | refill 필요 여부 + lock 획득 판정 | lockKey, balanceKey, threshold, traceId | 잔량, DB 고갈 플래그(is_empty), lock 키 | FAIL/SKIP/OK/WAIT |
| lock_heartbeat.lua | lock 소유자 TTL 연장 | lockKey, traceId, ttlMs | lock 키 | 1(성공)/0(실패) |
| lock_release.lua | lock 소유자 해제 | lockKey, traceId | lock 키 | 1(해제)/0(무시) |

- `deduct_*` 스크립트는 `is_empty`를 갱신하지 않으며, 해당 플래그는 DB claim 결과 기반으로만 관리한다.
- `refill_gate.lua`는 `balanceKey`에서 `is_empty`를 직접 읽어 SKIP/진입 여부를 단일 판정한다.

## **9) Lua 사용 순서**

1. 메시지 소비 후 dedupe:run:{traceId} 선점(SET NX EX).
2. 이벤트 요청량(`apiTotalData`)으로 deduct_indiv_tick.lua 호출.
3. 개인풀 status가 HYDRATE면 DB hydrate 후 같은 이벤트에서 개인풀 Lua 1회 재호출.
4. 개인풀 차감 후 residual data 계산.
5. 개인풀 status가 NO_BALANCE이고 `is_empty != 1`이면 refill_gate.lua 호출.
6. refill 결과가 OK면 DB row lock(`SELECT ... FOR UPDATE`)으로 `actual refill amount = min(refill unit, db remaining)`를 계산/차감하고, 같은 양으로 Redis를 충전한 뒤 개인풀 Lua를 **residual(`currentTickTargetData - 1차 answer`) 기준으로 1회 재호출**한다(heartbeat 유지).
7. 그래도 residual data > 0이고 개인풀 status가 NO_BALANCE면 deduct_shared_tick.lua 호출.
8. 공유풀도 HYDRATE/NO_BALANCE(`is_empty != 1` 조건)/refill 동일 규칙 적용.
9. 이벤트 단일 사이클 종료 후 최종 상태 저장.
11. 최종 상태 영속 저장 성공 후 XACK.
12. dedupe:run:{traceId} 정리.

## **10) residual 계산**

- 개인풀 차감 후:
- residual_data = max(api_total_data - individual_deducted, 0)

## **11) 정책 적용 순서**

### **11.1 공통**

- 화이트리스트면 차단 포함 모든 정책 우회 후 차감/집계만 수행
- 화이트리스트가 아니면 아래 순서대로 첫 제한에서 즉시 반환

### **11.2 개인풀**

- 즉시 차단 -> 반복 차단 -> 일 총 사용량 제한 -> 앱 일 사용량 제한 -> 앱 속도 제한

### **11.3 공유풀**

- 즉시 차단 -> 반복 차단 -> 일 총 사용량 제한 -> 월 공유풀 제한 -> 앱 일 사용량 제한 -> 앱 속도 제한

### **11.4 개인 데이터 차감 가능량 계산 및 차감량 반환 (상세)**

1. `currentTickTargetData < 0`이면 `{"answer":-1,"status":"ERROR"}` 반환
2. `answer = min(개인풀 잔량, currentTickTargetData)`
3. `answer == 0`이면 `{"answer":0,"status":"NO_BALANCE"}` 반환
4. 앱 화이트리스트 여부 확인
5. 화이트리스트 앱이면 11번으로 이동(차단/제한 정책 우회)
6. 즉시 차단 정책 적용 중이면 `{"answer":0,"status":"BLOCKED_IMMEDIATE"}` 반환
7. 반복 차단 상태이면 `{"answer":0,"status":"BLOCKED_REPEAT"}` 반환
8. 일 데이터 사용 제한 검증
- 정책 비활성 또는 제한값 `1`이면 다음 단계
- 아니라면 `answer = min(answer, max(0, 일 제한값 - 일 누적 사용량))`
- 이때 `answer == 0`이면 `{"answer":0,"status":"HIT_DAILY_LIMIT"}` 반환
9. 앱 데이터 사용 제한 검증
- 정책 비활성 또는 제한값 `1`이면 다음 단계
- 아니라면 `answer = min(answer, max(0, 앱 일 제한값 - 앱 일 누적 사용량))`
- 이때 `answer == 0`이면 `{"answer":0,"status":"HIT_APP_DAILY_LIMIT"}` 반환
10. 앱 속도 제한 검증
- 정책 비활성 또는 제한값 `1`이면 다음 단계
- 아니라면 `answer = min(answer, 초당 사용 가능 데이터량(Byte))`
- 이때 `answer == 0`이면 `{"answer":0,"status":"HIT_APP_SPEED"}` 반환
11. `answer`만큼 개인풀 잔량 차감
12. `answer`만큼 `daily_total_usage` 증가
13. `answer`만큼 `daily_app_usage` 증가
14. 정책에 의한 제한/차단이 한 번도 발생하지 않았을 때만 `{"answer":answer,"status":"OK"}` 반환

### **11.5 공유 데이터 차감 가능량 계산 및 차감량 반환 (상세)**

1. `currentTickTargetData < 0`이면 `{"answer":-1,"status":"ERROR"}` 반환
2. `answer = min(공유풀 잔량, currentTickTargetData)`
3. `answer == 0`이면 `{"answer":0,"status":"NO_BALANCE"}` 반환
4. 앱 화이트리스트 여부 확인
5. 화이트리스트 앱이면 12번으로 이동(차단/제한 정책 우회)
6. 즉시 차단 정책 적용 중이면 `{"answer":0,"status":"BLOCKED_IMMEDIATE"}` 반환
7. 반복 차단 상태이면 `{"answer":0,"status":"BLOCKED_REPEAT"}` 반환
8. 일 데이터 사용 제한 검증
- 정책 비활성 또는 제한값 `1`이면 다음 단계
- 아니라면 `answer = min(answer, max(0, 일 제한값 - 일 누적 사용량))`
- 이때 `answer == 0`이면 `{"answer":0,"status":"HIT_DAILY_LIMIT"}` 반환
9. 월 공유풀 사용 제한 검증
- 정책 비활성 또는 제한값 `1`이면 다음 단계
- 아니라면 `answer = min(answer, max(0, 월 공유 제한값 - 월 공유 누적 사용량))`
- 이때 `answer == 0`이면 `{"answer":0,"status":"HIT_MONTHLY_SHARED_LIMIT"}` 반환
10. 앱 데이터 사용 제한 검증
- 정책 비활성 또는 제한값 `1`이면 다음 단계
- 아니라면 `answer = min(answer, max(0, 앱 일 제한값 - 앱 일 누적 사용량))`
- 이때 `answer == 0`이면 `{"answer":0,"status":"HIT_APP_DAILY_LIMIT"}` 반환
11. 앱 속도 제한 검증
- 정책 비활성 또는 제한값 `1`이면 다음 단계
- 아니라면 `answer = min(answer, 초당 사용 가능 데이터량(Byte))`
- 이때 `answer == 0`이면 `{"answer":0,"status":"HIT_APP_SPEED"}` 반환
12. `answer`만큼 공유풀 잔량 차감
13. `answer`만큼 `daily_total_usage` 증가
14. `answer`만큼 `monthly_shared_usage` 증가
15. `answer`만큼 `daily_app_usage` 증가
16. 정책에 의한 제한/차단이 한 번도 발생하지 않았을 때만 `{"answer":answer,"status":"OK"}` 반환

### **12) 데이터 리필량 계산 (확정 규칙)**

참고 용어

- `delta`: 최근 데이터 평균 차감량(Byte)
- `refill unit`: 리필 단위
- `redis threshold`: 리필 임계치

1. 계산/갱신 타이밍:
    1. `refill_gate.lua` 호출 직전에 항상 최신 버킷으로 `delta/refill unit/redis threshold`를 재계산한다.
2. 10초 평균 산식:
    1. 최근 10초 버킷에서 `delta = ceil(sum(answer) / 존재 버킷 수)`를 계산한다.
    2. "존재 버킷 수"는 값이 실제로 존재하는 버킷 개수만 사용한다.
3. 데이터 부족 시 fallback 순서:
    1. 최근 10초 버킷이 없으면, TTL 내 현재 남은 전체 버킷으로 동일 산식 계산
    2. 전체 버킷도 없으면 `apiTotalData`를 fallback 값으로 사용
4. `refill unit`/`redis threshold` 계산 규칙:
    1. 기본: `refill unit = ceil(delta * 10)`, `redis threshold = ceil(delta * 3)`
    2. `delta == 0` 또는 버킷 정보 없음: `refill unit = apiTotalData`, `redis threshold = ceil(apiTotalData * 0.3)`
    3. 계산 결과는 음수가 될 수 없고, `redis threshold` 최소값은 1로 보정한다.
5. 리필 신호 조건:
    1. 현재 Redis 잔량(`remaining_*_amount`)이 `redis threshold` 미만이고, `is_empty != 1`이면 `refill_gate.lua`로 리필 시도를 진행한다.
6. 버킷 스코프:
    1. 개인풀은 lineId 기준 버킷 집합으로 계산한다.
    2. 공유풀은 familyId 기준 버킷 집합으로 계산한다.

### **12.0.1) 리필 관측 로그 필드 (확정)**

1. 리필 계획 계산 로그:
    1. `traceId`, `poolType`, `balanceKey`, `delta`, `bucketCount`, `refillUnit`, `redisThreshold`, `source`
2. DB 리필 적용/미적용 로그:
    1. `traceId`, `poolType`, `requestedRefill`, `actualRefill`, `dbRemainingBefore`, `dbRemainingAfter`

### **12.1) DB refill 원자 처리 규칙 (추가 확정)**

1. 진입 조건:
    1. 현재 tick 결과가 `NO_BALANCE`
    2. Redis 잔량 키의 `is_empty != 1`
    3. `refill_gate.lua` 결과가 `OK`
    4. lock 소유권(heartbeat)이 유효함
2. DB 차감 트랜잭션:
    1. 대상 풀 레코드를 `SELECT ... FOR UPDATE`로 조회한다.
    2. `actual refill amount = min(refill unit, db remaining)`를 계산한다.
    3. `actual refill amount <= 0`이면 DB/Redis 변경 없이 리필을 종료하고 현재 tick 결과는 `NO_BALANCE`를 유지한다.
    4. `actual refill amount > 0`이면 DB 잔량을 `actual refill amount`만큼 차감한다.
3. Redis 반영:
    1. DB 차감 성공 후 Redis 잔량 키를 `actual refill amount`만큼 증가시킨다.
    2. Redis 충전량은 반드시 DB 차감량과 동일해야 한다.
    3. `is_empty`는 DB claim 결과(`dbRemainingAfter`)로만 확정한다(`<=0`이면 `1`, `>0`이면 `0`).
4. tick 재시도:
    1. Redis 반영이 성공한 경우에만 같은 tick에서 차감 Lua를 **residual 기준으로 1회 재호출**한다.

### **12.2) DB/Redis 불일치 방지 규칙 (추가 확정)**

1. 기본 전략:
    1. 동기 경로(같은 tick)에서 DB 차감 -> Redis 충전을 우선 시도한다.
2. outbox(ledger) 보강:
    1. DB 차감 성공 후 Redis 충전 실패 시 `refill_ledger`에 재처리 상태를 기록한다.
    2. 백그라운드 재처리 워커가 ledger를 읽어 Redis 충전을 재시도한다.
3. idempotency:
    1. ledger 유니크 키는 `(trace_id, tick, pool_type)`로 고정한다.
    2. 동일 키는 한 번만 "DB 차감 완료" 상태가 될 수 있다.
4. 상태 전이:
    1. `INIT -> DB_DEDUCTED -> REDIS_APPLIED`를 정상 경로로 사용한다.
    2. Redis 실패 시 `DB_DEDUCTED -> REDIS_PENDING`으로 전이하고 재처리 성공 시 `REDIS_APPLIED`로 전이한다.

### **12.3) DB 예외/재시도 규칙 (추가 확정)**

1. 재시도 대상:
    1. deadlock
    2. lock wait timeout
2. 재시도 정책:
    1. 최대 2회 재시도
    2. 재시도 간 짧은 backoff를 둔다(예: 50ms)
3. 비재시도 대상:
    1. 스키마/구문/매핑 오류
    2. 무효 파라미터
    3. 기타 비일시적 예외
4. 재시도 소진 또는 비재시도 예외 발생 시:
    1. 해당 tick에서는 리필을 중단하고 기존 `NO_BALANCE` 결과를 유지한다.
    2. lock은 소유자 기준으로 반드시 해제한다.

### **12.4) 월 경계/삭제 상태 규칙 (추가 확정)**

1. 월 기준:
    1. `enqueuedAt` 기준 월을 DB/Redis 모두 동일하게 사용한다.
2. 엔티티 누락:
    1. line/family 레코드가 없거나 soft-delete 상태면 리필 불가로 처리한다.
3. 데이터 이상:
    1. DB 잔량이 음수인 비정상 상태는 데이터 오류로 기록하고 리필을 중단한다.
4. 공통 원칙:
    1. 월 키와 DB 조회 월이 불일치하면 리필을 진행하지 않는다.
    2. 정합성보다 가용성을 우선해 중복 차감을 허용하지 않는다.

## **13) 키 미존재/HYDRATE 규칙**

- key 미존재 시 Lua는 HYDRATE 반환
- 트래픽 처리 서버는 DB hydrate 후 같은 이벤트 처리 사이클에서 1회 재시도
- 앱 정책 미존재는 무제한으로 간주
- hydrate 실패 또는 재실패 시 ERROR, 5xx 로그 기록 후 종료

## **14) 회복 불가 상태 처리(처리 불가 상태)**

- 아래 상태는 회복 불가로 간주하고 로그 후 즉시 종료:
- BLOCKED_IMMEDIATE
- BLOCKED_REPEAT
- HIT_DAILY_LIMIT
- HIT_MONTHLY_SHARED_LIMIT
- HIT_APP_DAILY_LIMIT
- ERROR

## **15) 동시성/정합성 규칙**

- Lua는 읽기/검증/차감/갱신을 원자 수행
- 처리 전제:
- 정상 시나리오에서는 같은 `lineId + appId` 조합이 동시에 실행되는 경우가 논리적으로 발생하지 않는다.
- 예외 시나리오(재전달/중복 전달/재시도 경쟁)에서는 Redis dedupe 및 DONE idempotency로 정합성을 보장한다.
- 하나의 데이터 처리 요청 이벤트는 단일 스레드에서 순차 실행한다.
- dedupe는 Redis(in-flight) + 영속 저장소(DONE) 이중 보장
- refill은 Redis lock + DB row lock(`SELECT ... FOR UPDATE`)의 이중 잠금으로 보호한다.
- DB 차감량과 Redis 충전량의 불일치는 refill ledger 재처리 대상으로 관리한다.
- `XACK`는 반드시 DONE 영속 저장 이후 수행

## **16) 최종 데이터 차감 플로우 (개발 착수용)**

1. API 서버가 요청 수신 후 traceId 생성, MQ 적재.
2. 트래픽 처리 서버가 메시지 소비, in-flight dedupe 선점.
3. api_remaining_data = api_total_data 초기화.
4. 개인풀 Lua 실행.
5. HYDRATE면 hydrate + 개인풀 재시도 1회.
6. NO_BALANCE이고 `is_empty != 1`이면 refill gate 실행.
7. OK로 lock 획득 시 DB row lock(`SELECT ... FOR UPDATE`)으로 `actual refill amount=min(refill unit, db remaining)`를 계산/차감하고, 같은 양으로 Redis를 충전한 뒤 개인풀을 **residual 기준으로 재시도 1회**한다.
8. 개인풀 처리 후 residual_data > 0이고 status가 NO_BALANCE면 공유풀 Lua 실행.
9. 공유풀에서도 HYDRATE/NO_BALANCE(`is_empty != 1` 조건)/refill 동일 처리.
10. 이벤트 단일 사이클 종료 후 결과 상태 계산:
    - api_remaining_data == 0 -> SUCCESS
    - api_remaining_data > 0 -> PARTIAL_SUCCESS
    - 시스템 예외 -> FAILED
11. 최종 결과를 영속 저장소에 기록.
12. 영속 저장 성공 후 XACK.
13. in-flight dedupe 정리.

### 16) 수정본(가독성 향상)

1. 트래픽 처리 서버가 MQ 메시지(traceId, lineId, familyId, appId, apiTotalData)를 수신한다.
2. dedupe:run:{traceId}를 선점한다. 실패 시 중복 처리 정책에 따라 스킵/종료한다.
3. api_remaining_data = apiTotalData로 초기화한다.
4. `deduct_indiv_tick.lua`를 호출한다.
5. Lua는 원자적으로 검증 -> answer 계산 -> 차감/집계 -> JSON(`{"answer":...,"status":"..."}`) 반환을 수행한다.
6. 개인풀 결과가 HYDRATE면 DB hydrate 후 같은 이벤트 처리 사이클에서 `deduct_indiv_tick.lua`를 1회 재호출한다.
7. 개인풀 결과가 NO_BALANCE이고 `is_empty != 1`이면 `refill_gate.lua`를 호출한다.
8. refill 결과가 OK면 DB row lock(`SELECT ... FOR UPDATE`)으로 `actual refill amount=min(refill unit, db remaining)`를 계산/차감하고, 같은 양으로 Redis를 충전한 뒤 같은 이벤트 처리 사이클에서 `deduct_indiv_tick.lua`를 **residual 기준으로 1회 재호출**한다.
9. 개인풀 처리 후 `residual_data = max(apiTotalData - indiv_answer_total, 0)`를 계산한다.
10. residual_data > 0이고 개인풀 최종 status가 NO_BALANCE인 경우에만 `deduct_shared_tick.lua`를 호출한다.
11. 공유풀도 동일하게 HYDRATE -> hydrate 1회 재호출, NO_BALANCE(`is_empty != 1`) -> refill_gate -> 필요 시 **residual 기준 재호출** 규칙을 적용한다.
12. 이벤트 단일 처리 종료 후 api_remaining_data == 0이면 SUCCESS, 아니면 PARTIAL_SUCCESS, 시스템 예외면 FAILED를 결정한다.
13. 최종 결과를 영속 저장소에 저장한다(DONE).
14. DONE 저장 성공 후 XACK한다.
15. dedupe:run:{traceId}를 정리하고 종료한다.

## **17) 프로파일 운영 규칙 (추가 확정)**

- 프로파일 종류: `local`, `api`, `traffic`
- `local` 프로파일: 개발용. API 서버 역할 + 트래픽 처리 서버 역할을 모두 수행한다.
- `api` 프로파일: API 서버 역할만 수행한다.
    - 요청 수신 -> `traceId` 생성 -> Streams(MQ) 적재까지 수행한다.
    - cache용 Redis에는 접근하지 않으며, streams용 Redis에는 접근한다.
    - 세션용 Redis는 별도 인프라이며 기존 설정대로 사용한다.
- `traffic` 프로파일: 트래픽 처리 서버 역할만 수행한다.
    - MQ/Streams 소비, 이벤트 단일 사이클 차감 오케스트레이션, DONE 저장, XACK 수행
    - cache용 Redis, streams용 Redis 모두 접근한다.
- 배포 방식: 동일 애플리케이션 아티팩트를 프로파일만 다르게 실행한다.
    - 예: `SPRING_PROFILES_ACTIVE=api` 또는 `SPRING_PROFILES_ACTIVE=traffic`

## **18) Redis 인프라 분리 및 설정 키 (추가 확정)**

- Redis 인프라는 아래 3개를 완전히 분리한다.
    - 세션용 Redis
    - 캐시/정책/카운터용 Redis
    - Streams용 Redis
- 설정 값은 모두 `.env` 환경변수로 주입한다.
- 공통 키 네임스페이스(prefix)는 통일한다.
    - 예: `${REDIS_NAMESPACE}`
    - 키 예시: `${REDIS_NAMESPACE}:policy:{policyId}`, `${REDIS_NAMESPACE}:dedupe:run:{traceId}`

권장 프로퍼티 키(값은 `${필드명}` 주입):

- 세션 Redis
    - `spring.data.redis.host: ${SESSION_REDIS_HOST}`
    - `spring.data.redis.port: ${SESSION_REDIS_PORT}`
    - `spring.data.redis.password: ${SESSION_REDIS_PASSWORD}`
- 캐시 Redis
    - `app.redis.cache.host: ${CACHE_REDIS_HOST}`
    - `app.redis.cache.port: ${CACHE_REDIS_PORT}`
    - `app.redis.cache.password: ${CACHE_REDIS_PASSWORD}`
- Streams Redis
    - `app.redis.streams.host: ${STREAMS_REDIS_HOST}`
    - `app.redis.streams.port: ${STREAMS_REDIS_PORT}`
    - `app.redis.streams.password: ${STREAMS_REDIS_PASSWORD}`

## **19) Streams 소비/ACK 전략 (추가 확정)**

- 메인 소비는 스케줄러 기반 n초 폴링이 아니라, **블로킹 기반 지속 소비**를 기본으로 한다.
    - 권장: `XREADGROUP ... BLOCK <ms>`
- 스케줄러는 보조 작업(예: pending reclaim/XAUTOCLAIM) 용도로만 사용한다.
- ACK 순서 고정:
    - DONE 영속 저장 성공 후 `XACK`
    - 실패 시 `XACK` 금지

## **20) Streams 권장 초기값 (추가 확정)**

- stream key: `${STREAMS_KEY_TRAFFIC_REQUEST}` (예: `traffic:deduct:request`)
- consumer group: `${STREAMS_GROUP_TRAFFIC}` (예: `traffic-deduct-cg`)
- consumer name: `${STREAMS_CONSUMER_NAME}` (예: `${HOSTNAME}-${SERVER_PORT}`)
- read batch size: `${STREAMS_READ_COUNT}` (권장 초기값: `20`)
- read block ms: `${STREAMS_BLOCK_MS}` (권장 초기값: `2000`)
- reclaim interval ms: `${STREAMS_RECLAIM_INTERVAL_MS}` (권장 초기값: `5000`)
- reclaim min idle ms: `${STREAMS_RECLAIM_MIN_IDLE_MS}` (권장 초기값: `70000`)
    - 근거: `INFLIGHT_TTL_SEC=60`보다 여유를 둬 중복 경쟁을 줄인다.
- max retry: `${STREAMS_MAX_RETRY}` (권장 초기값: `3`)
- DLQ key: `${STREAMS_KEY_TRAFFIC_DLQ}` (예: `traffic:deduct:dlq`)

## **21) Streams JSON 메시지 처리 규칙 (추가 확정)**

- Streams 메시지는 JSON 기반으로 처리한다.
- 권장 저장 형식:
    - field: `payload`
    - value: JSON 문자열
- `payload` JSON에는 최소 아래 필드를 포함한다.
    - `traceId`, `lineId`, `familyId`, `appId`, `apiTotalData`, `enqueuedAt`
- 트래픽 처리 서버는 아래 순서로 처리한다.
    1. Streams 레코드 수신
    2. `payload` 문자열 추출
    3. DTO 역직렬화(JSON -> 객체)
    4. 필수 필드 검증
    5. 차감 로직 실행
    6. DONE 저장 성공 시 `XACK`
    7. 역직렬화/검증 실패 또는 재시도 초과 시 DLQ 적재

## **22) 정책 변경 시 캐시 갱신 규칙 (추가 확정)**

- 정책 변경 시 Redis 반영은 **즉시 갱신**이 원칙이다.
- 변경 대상 정책 키는 지연 없이 갱신(write-through 또는 동등한 즉시 반영 방식)한다.
- 즉시 갱신 실패 시 재시도/오류 로그를 남기고, 불일치 상태를 방치하지 않는다.
