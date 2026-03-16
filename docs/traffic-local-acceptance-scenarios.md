# Traffic 로컬 인수테스트 시나리오 (정합성 + Redis Failover)

## 1) 목적
- 대상: `family_id=1`, `line_id=1~4`
- 방식: 각 시나리오마다 트래픽 처리 API(`/api/traffic/requests`) 호출
- 공통 사전조건:
  - Redis `flushall` (cache, streams 둘 다)
  - `FAMILY.pool_remaining_data=100` 초기화
  - `LINE(1~4).remaining_data=200` 초기화
  - 필요 시 정책 레코드(`LINE_LIMIT`, `APP_POLICY`, `REPEAT_BLOCK*`) 실제 DB 삽입
- 실행 환경 제한:
  - `local` 프로파일에서만 실행
  - CI/운영에서는 실행 금지

---

## 2) 파이프라인 분해 기준
1. API ingress + stream enqueue
2. stream consume + dedupe + done log
3. line 정책 hydrate(온디맨드)
4. Lua 차감(개인/공유) 정책 판정
5. HYDRATE 재시도
6. REFILL gate/lock + DB claim + Redis apply
7. Outbox 상태 전이(PENDING/SUCCESS/FAIL/REVERT)
8. 정책 글로벌 on/off 전파(`pooli:policy:*`)

---

## 3) 공통 검증 포인트
- DB:
  - `LINE.remaining_data`
  - `FAMILY.pool_remaining_data`
  - `TRAFFIC_REDIS_OUTBOX` 상태/재시도 횟수
- Redis:
  - `pooli:remaining_indiv_amount:{lineId}:{yyyymm}` hash(`amount`, `is_empty`)
  - `pooli:remaining_shared_amount:{familyId}:{yyyymm}` hash(`amount`, `is_empty`)
  - `pooli:daily_total_usage:{lineId}:{yyyymmdd}`
  - `pooli:daily_app_usage:{lineId}:{yyyymmdd}`
  - `pooli:policy:{1..7}` hash(`value`, `version`)
  - `pooli:line_policy_ready:{lineId}`

---

## 4) 시나리오 매트릭스

### A. 기본 흐름/정합성
- A-01: 기본 개인풀 차감 성공
  - setup: 기본 초기화
  - call: line 1, app 1, `apiTotalData=50`
  - expect: `LINE.remaining_data`가 50 감소, outbox REFILL 성공, Redis 개인풀 키 생성
- A-02: 개인풀 부족 시 공유풀 보완 차감
  - setup: `LINE(1).remaining_data=0`, family 100
  - call: line 1, family 1, app 1, 50
  - expect: `FAMILY.pool_remaining_data` 감소, 개인풀 DB는 0 유지
- A-03: 요청량 0
  - setup: 기본 초기화
  - call: `apiTotalData=0`
  - expect: DB/Redis 차감 없음, 오류 없이 종료
- A-04: 음수/잘못된 payload
  - setup: 기본 초기화
  - call: `apiTotalData=-1` 또는 필수값 누락
  - expect: 처리 실패, 차감 없음, DLQ/에러 경로 확인

### B. 정책 hydrate 정상/실패
- B-01: flushall 직후 첫 요청에서 line policy hydrate 성공
  - setup: flushall 후 즉시 요청
  - expect: `line_policy_ready:{lineId}` 생성, 요청 처리 성공
- B-02: line policy hydrate lock 경합
  - setup: 같은 line_id로 동시 2요청
  - expect: 중복 hydrate 없이 한쪽 선점, 둘 다 정합성 유지
- B-03: hydrate 재시도 소진
  - setup: hydrate lock을 강제로 선점 고정
  - expect: HYDRATE retry max 후 실패 상태 반환, 과차감 없음

### C. 정책 on/off 전파 및 회선별 적용 비활성화 검증
- C-01: `policy:2`(즉시차단) ON + block_end_at 미래
  - expect: `BLOCKED_IMMEDIATE`, DB 차감 없음
- C-02: `policy:2` OFF + 동일 line block 설정 유지
  - expect: 차단 우회, 차감 진행
- C-03: `policy:1`(반복차단) ON + 현재 요일/시간 블록 레코드 삽입
  - expect: `BLOCKED_REPEAT`
- C-04: `policy:1` OFF + 동일 반복차단 레코드 유지
  - expect: 차단 우회
- C-05: `policy:4`(일일총량) ON + `LINE_LIMIT.daily_data_limit=30`
  - expect: 최대 30까지만 차감
- C-06: `policy:4` OFF + 동일 `LINE_LIMIT` 유지
  - expect: 일일총량 제한 미적용
- C-07: `policy:3`(월공유한도) ON + `LINE_LIMIT.shared_data_limit=20`
  - expect: 공유풀 차감 시 월공유 제한 적용
- C-08: `policy:3` OFF + 동일 `LINE_LIMIT` 유지
  - expect: 월공유 제한 미적용
- C-09: `policy:5`(앱 데이터) ON + `APP_POLICY(data_limit=10, active=1)` 삽입
  - expect: 앱별 데이터 제한 적용
- C-10: `policy:5` OFF + 동일 APP_POLICY 유지
  - expect: 앱 데이터 제한 미적용
- C-11: `policy:6`(앱 속도) ON + `APP_POLICY(speed_limit=10, active=1)` 삽입
  - expect: 앱 속도 제한 적용(`HIT_APP_SPEED`)
- C-12: `policy:6` OFF + 동일 APP_POLICY 유지
  - expect: 속도 제한 미적용
- C-13: `policy:7`(화이트리스트) ON + `APP_POLICY(is_whitelist=1, active=1)` 삽입
  - expect: whitelist 앱은 차단/한도 정책 우회
- C-14: `policy:7` OFF + 동일 whitelist 레코드 유지
  - expect: whitelist 우회 비활성

### D. 복합 정책 상호작용
- D-01: 즉시차단 + 앱화이트리스트 동시 활성
  - expect: whitelist 앱은 차단 우회, non-whitelist 앱은 차단
- D-02: 반복차단 + 일일총량 동시 활성
  - expect: 차단 시간대는 BLOCKED, 비차단 시간대는 일일한도 적용
- D-03: 앱데이터 + 앱속도 동시 활성
  - expect: 더 먼저 소진되는 제한 원인으로 상태 귀결
- D-04: 개인풀 부족 + 공유풀 + 월공유한도 + 앱데이터 동시
  - expect: 잔량/한도 우선순위대로 partial/success 결정
- D-05: policy 글로벌 일부 OFF(예: 4,5) + 회선 정책 레코드 유지
  - expect: OFF된 정책만 우회, 나머지 정책은 계속 적용

### E. Redis Failover/복구 정합성
- E-01: DB claim 성공 후 Redis apply 실패
  - setup: REFILL apply 직전에 cache redis 연결 차단
  - expect: outbox `FAIL` 또는 `PENDING`, DB 반납/재시도 경로 진입
- E-02: Redis timeout 장애
  - setup: tc/netem 또는 proxy로 timeout 유도
  - expect: outbox fail 증가, 중복 차감 없음
- E-03: retry scheduler 1회 복구 성공
  - setup: E-01 상태에서 redis 복구 후 scheduler 실행 대기
  - expect: outbox `SUCCESS`, idempotency key 정리
- E-04: retry max 초과
  - setup: redis 장시간 차단
  - expect: REFILL은 `REVERT` 전이 + DB 원복, 비-REFILL은 terminal fail marker
- E-05: 처리 중 lock 유실
  - setup: refill lock ttl 만료 유도
  - expect: 보상(compensate) 1회 수행, 과차감 없음
- E-06: 동일 refill UUID 중복 재처리
  - setup: outbox 재시도 중복 실행
  - expect: idempotent skip, amount 중복 증가 없음

### F. 중복 전달/재전달/멱등성
- F-01: 동일 traceId 중복 전송
  - expect: done-log/ dedupe로 1회만 차감
- F-02: ACK 직전 예외로 pending 전환 후 reclaim
  - expect: 재처리되어도 최종 1회 차감 유지
- F-03: payload 파손/traceId 누락
  - expect: DLQ 기록 + ACK, 본 처리 미수행

---

## 5) 실행 우선순위(권장)
1. A-01~A-04 (기본 정합성)
2. C-01~C-14 (정책 on/off 전파 검증)
3. D-01~D-05 (복합 정책)
4. E-01~E-06 (failover/복구)
5. F-01~F-03 (멱등성/재전달)

---

## 6) 현재 코드 반영 상태
- 구현됨: `src/test/java/com/pooli/traffic/acceptance/TrafficFlowLocalAcceptanceTest.java`
  - 로컬 전용 실행(`@ActiveProfiles("local")`, CI 차단)
  - 매 테스트 `flushall` + family/line 잔량 초기화
  - 정책 on/off 전파 핵심 시나리오 포함
- 추가 필요: E/F 시나리오(네트워크 장애 유도/재시도 제어) 확장 테스트
