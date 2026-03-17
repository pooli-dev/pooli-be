# 트래픽 정책 검증 테스트 절차 가이드

이 문서는 아래 스크립트를 사용해 **사전 준비 → 데이터 셋업 → 부하 실행(k6) → 검증**까지 한 번에 수행하는 절차를 설명합니다.

- `scripts/traffic/setup_traffic_test_100.sh`
- `scripts/traffic/setup_traffic_test_1000.sh`
- `scripts/traffic/k6_traffic_test_100.js`
- `scripts/traffic/k6_traffic_test_1000.js`
- `scripts/traffic/verify_remaining_consistency_100.sh`
- `scripts/traffic/verify_remaining_consistency_1000.sh`

## 1. 사전 준비

### 1.1 실행 위치
아래 모든 명령은 프로젝트 루트에서 실행합니다.

```bash
cd /Users/kjh/Documents/pooli
```

### 1.2 필수 도구
아래 CLI가 필요합니다.

- `mysql`
- `redis-cli`
- `mongosh`
- `k6`

확인 예시:

```bash
mysql --version
redis-cli --version
mongosh --version
k6 version
```

### 1.3 `.env` 점검
검증 스크립트는 기본적으로 프로젝트 루트의 `.env`를 사용합니다.

필수 항목:

- `DB_URL`
- `DB_USERNAME`
- `DB_PASSWORD`
- `CACHE_REDIS_HOST`
- `CACHE_REDIS_PORT`

권장 항목:

- `CACHE_REDIS_PASSWORD` (설정된 경우)
- `REDIS_NAMESPACE` (미설정 시 기본값: `pooli`)
- `MONGO_URI` 또는 `LOCAL_MONGO_URI`
- `MONGO_DB_NAME` (미설정 시 기본값: `pooli`)

### 1.4 서버 상태 확인
테스트 전 아래가 모두 통신 가능해야 합니다.

- Spring Boot API 서버 (`http://localhost:8080`)
- MySQL
- Redis(cache)
- MongoDB

간단 체크 예시:

```bash
curl -i http://localhost:8080/actuator/health
redis-cli -h "$CACHE_REDIS_HOST" -p "$CACHE_REDIS_PORT" ping
```

## 2. 테스트 시나리오 선택

### 2.1 100 라인 시나리오
- 라인 수: 100
- 패밀리 수: 25 (가족당 4라인)

그룹 범위:

- G1: `1~25`
- G2: `26~50`
- G3: `51~75`
- G4: `76~87`
- G5: `88~100`

### 2.2 1000 라인 시나리오
- 라인 수: 1000
- 패밀리 수: 250

그룹 범위:

- G1: `1~250`
- G2: `251~500`
- G3: `501~750`
- G4: `751~875`
- G5: `876~1000`

## 3. Step-by-step 실행 절차

### 3.1 Step 1 - 테스트 데이터 셋업(캐시 초기화 포함)

이 단계는 반드시 매 테스트 실행 전에 수행하세요.

- Redis에서 `${REDIS_NAMESPACE}:*` 키를 삭제
- 이어서 선택한 SQL 셋업 파일 적용

100 라인:

```bash
./scripts/traffic/setup_traffic_test_100.sh
```

1000 라인:

```bash
./scripts/traffic/setup_traffic_test_1000.sh
```

### 3.2 Step 2 - 애플리케이션 실행

로컬 프로파일로 API 서버를 실행합니다. IDE 실행 또는 CLI 실행 중 하나를 사용하세요.

CLI 예시:

```bash
./gradlew bootRun --args='--spring.profiles.active=local'
```

서버가 기동된 뒤 health check가 `UP`인지 확인합니다.

### 3.3 Step 3 - k6 부하 테스트 실행

기본 타겟 URL은 `http://localhost:8080/api/traffic/requests` 입니다.

100 라인:

```bash
k6 run scripts/traffic/k6_traffic_test_100.js
```

1000 라인:

```bash
k6 run scripts/traffic/k6_traffic_test_1000.js
```

필요 시 URL/타임아웃 오버라이드:

```bash
BASE_URL=http://localhost:8080 REQUEST_TIMEOUT=30s k6 run scripts/traffic/k6_traffic_test_100.js
```

### 3.4 Step 4 - 일관성 검증 실행

100 라인:

```bash
./scripts/traffic/verify_remaining_consistency_100.sh
```

1000 라인:

```bash
./scripts/traffic/verify_remaining_consistency_1000.sh
```

## 4. 검증 항목 해석

검증 스크립트는 아래 4개 섹션을 출력합니다.

1. `Line Remaining (MySQL + Redis)`
2. `Family Shared Remaining (MySQL + Redis)`
3. `MySQL Outbox/Fallback Tables`
4. `Mongo Done-Log`

마지막 줄이 아래처럼 나오면 성공입니다.

```text
FINAL RESULT: PASS (all consistency checks matched)
```

실패 시 예시:

```text
FINAL RESULT: FAIL (N mismatched checks found)
```

그리고 각 라인의 `FAIL`을 보고 어느 그룹(G1~G5)에서 불일치가 났는지 추적하면 됩니다.

## 5. 현재 검증 로직 기준(중요)

### 5.1 G4 검증
- 라인별 차감량은 분배 비결정성이 있어 고정 기대값으로 판정하지 않습니다.
- 대신 가족 단위 합산 차감량으로 최종 PASS/FAIL을 판정합니다.

### 5.2 G5 검증
- 시간 버킷 방식이 아닙니다.
- `app_speed_limit:{lineId}` 해시의 `speed:2` 값을 **요청 1건당 상한 바이트**로 사용합니다.
- k6와 동일한 deterministic chunk 계획으로 예상 차감량을 계산해 **정확값**으로 검증합니다.

## 6. 추천 실행 루틴

1. `setup_traffic_test_XXX.sh` 실행
2. API 서버 기동 확인
3. `k6 run ...` 실행
4. `verify_remaining_consistency_XXX.sh` 실행
5. FAIL 발생 시 바로 재실행하지 말고, 먼저 Step 1(셋업)부터 다시 수행

## 7. 자주 발생하는 문제

- Redis 연결 실패: `CACHE_REDIS_HOST/PORT`, 방화벽, Docker 포트 바인딩 확인
- DB 연결 실패: `DB_URL`, 포트, 계정 권한 확인
- 이전 테스트 잔재 영향: 반드시 Step 1로 캐시/데이터 초기화 후 재시도
