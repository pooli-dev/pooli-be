# **Redis Failover Outbox 패턴 요구사항 정의서**

## **1. 배경 및 목적**

### **1.1 현재 구조**

본 프로젝트는 DB-Redis 간 **Write-Through** 동기화 방식을 채택합니다.

- 변경사항은 **DB에 먼저 반영**된 후, **TrafficPolicyWriteThroughService**의 메서드를 통해 Redis로 동기화됩니다.
- Redis의 정책 관련 키는 **Hydration** 방식으로 적재됩니다. Redis가 다운된 이후 복구되면, Hydration이 DB의 최신 상태를 Redis에 업로드합니다.
- 데이터 잔량 리필이란, **DB에서 일정량의 잔량값을 차감하여 Redis에 가산**하는 작업입니다. Hydration 시 잔량은 0으로 초기화된 후 리필 로직이 즉시 수행됩니다.

### **1.2 목적**

- **Failover 방어**: Redis 장애(Down, Timeout) 시 동기화 실패를 기록하고 복구 재시도를 자동화합니다.
- **멱등성 보장**: 동일 요청이 중복 처리되더라도 결과에 차이가 없도록 방어합니다.
- **재시도 자동화**: 스케줄러 기반의 재처리를 통해 정합성을 유지합니다.

---

## **2. Outbox 테이블 스키마**

| **컬럼** | **타입** | **제약** | **설명** |
| --- | --- | --- | --- |
| `id` | BIGINT | PK, AUTO_INCREMENT | 기본 키 |
| `event_type` | VARCHAR | NOT NULL | 호출할 Write-Through 메서드 식별자 (예: `SYNC_LINE_LIMIT`, `SYNC_APP_POLICY`) |
| `payload` | TEXT (JSON) | NOT NULL | 메서드 호출 파라미터 JSON 직렬화. `version` 또는 `uuid` 포함 |
| `uuid` | VARCHAR | NULL | 리필 케이스 멱등키. Payload에도 포함 |
| `status` | VARCHAR | NOT NULL | `PENDING` / `PROCESSING` / `SUCCESS` / `FAIL` |
| `retry_count` | INT | NOT NULL, DEFAULT 0 | 스케줄러 재시도 누적 횟수 |
| `created_at` | DATETIME | NOT NULL | 레코드 최초 생성 일시 |
| `status_updated_at` | DATETIME | NOT NULL | status 가 마지막으로 변경된 일시. status 변경 시마다 갱신 |

---

## **3. Write-Through 최초 수행 흐름**

```
[요청 인입]
→ DB 수정
→ Outbox 레코드 삽입 (status = PENDING)
→ 트랜잭션 커밋
→ Redis 동기화 로직 수행
→ 결과에 따라 Outbox status 갱신 (SUCCESS or FAIL)
```

> **PENDING 레코드는 스케줄러의 재시도 대상입니다.** Redis 동기화 완료 전 애플리케이션 프로세스 크래시에 대한 안전망 역할을 합니다.
>

---

## **4. 에러 유형별 처리 원칙**

### **4.1 Connection Failure**

Redis 서버 자체에 연결할 수 없는 상태입니다.

**정책 수정 케이스:**

- 정책 키 또한 Hydration 대상이므로, Redis 복구 시 DB 최신 상태가 자동으로 재적재됩니다.
- Connection Failure 발생 시 Outbox status를 `SUCCESS`로 기록합니다. (Hydration이 복구를 보장하므로 재시도 불필요)

**데이터 리필 케이스:**

- Redis에 잔량을 가산할 수 없으므로, **DB에서 차감했던 잔량을 즉시 반납(원상복구)합니다.**
- 반납 후 Outbox status는 `SUCCESS`로 기록합니다. (Redis 복구 시 Hydration이 0 초기화→리필 수행하므로 이중 리필 위험 없음)

### **4.2 Timeout 및 그 외 일시적 오류**

- 요청이 Redis에 도달했는지 확인할 수 없으므로, Outbox status를 `FAIL`로 기록하고 **재시도 스케줄러 대상**에 편입합니다.

---

## **5. 케이스별 멱등성 보장 전략**

### **5.1 정책 수정 — Version 기반**

**Version 값의 정의:**

- Spring 서버가 최초로 요청을 받은 시점의 `LocalDateTime`을 **Asia/Seoul 기준 Epoch Second**로 변환한 값.
- Payload의 `version` 필드에 저장합니다.

**Redis 반영 조건 (Lua 스크립트 내 원자적 처리):**

```
lua

-- KEYS[1]: 대상 Redis Key
-- KEYS[2]: 요청 version 값
-- KEYS[3]: 저장할 value
localcurrent_version=redis.call('HGET',KEYS[1],'version')
ifcurrent_version==falseortonumber(KEYS[2])>tonumber(current_version)then
redis.call('HMSET',KEYS[1],'version',KEYS[2],'value',KEYS[3])
return1-- 갱신 성공
end
return0-- 현재 Redis가 더 최신, 기각
```

- GET-비교-SET을 단일 Lua 스크립트로 묶어 **원자적으로 처리**하여 Race Condition을 방지합니다.
- 단일 값으로 저장 중인 키는 Hash 구조로 변경(`HSET`)하여 `version` 필드를 수용합니다.

**SUCCESS 조건:**

- Redis 업데이트 정상 완료.
- Version 검증 실패(현재 Redis 버전이 더 높음) → 이미 최신 상태이므로 **SUCCESS로 간주**.

### **5.2 데이터 잔량 리필 — UUID 기반**

**UUID 역할:**

- 고유 멱등키로 사용됩니다.
- Outbox 레코드의 `uuid` 컬럼 및 `payload` 내에 저장합니다.

**Redis 멱등키 처리 (원자적 등록):**

```
SET pooli:refill:idempotency:{uuid} 1 EX 60 NX
```

- `NX` 조건으로 고유 키를 원자적으로 생성하며, **TTL 60초를 생성 시점에 동시에 부여**합니다.
- TTL 60초는 Redis가 최근 1분 이내 처리된 리필 요청의 멱등키를 보유하는 기간입니다.

**SUCCESS 조건:**

- 멱등키 등록 성공 & 리필 연산 정상 완료.
- 멱등키 등록 실패(`NX` 실패, 키 이미 존재): **중복 요청으로 간주하여 로직 미수행 후 SUCCESS 기록**.

---

## **6. 스케줄러 (재시도 배치)**

### **6.1 실행 환경**

- `@Profile("traffic")` 서버에서만 실행합니다.
- **다중 워커 동시 실행 허용**: `FOR UPDATE SKIP LOCKED`를 통해 레코드 단위 점유를 보장합니다.
- n초 주기 폴링.

### **6.2 재시도 대상 조회 조건 (SKIP LOCK 기반)**

다음 세 조건 중 하나를 만족하는 레코드를 조회합니다.

| **조건** | **대상 status** | **추가 조건** | **의도** |
| --- | --- | --- | --- |
| 순수 실패 건 | `FAIL` | 없음 | 재시도 |
| 지연된 PENDING 건 | `PENDING` | `created_at < NOW() - 1분` | 프로세스 크래시 등으로 상태 미갱신 복구 |
| 고착된 PROCESSING 건 | `PROCESSING` | `status_updated_at < NOW() - 5분` | 워커 크래시로 고착된 좀비 레코드 복구 |

> **생성된 지 1분 이내인 PENDING 레코드는 스케줄러 조회 대상에서 완전히 제외합니다.**
>
>
> (애플리케이션 스레드가 현재 Redis 동기화를 진행 중일 수 있으므로 개입을 차단합니다.)
>

### **6.3 재시도 처리 플로우 (2-Phase Transaction)**

```
[Phase 1] — 독립 트랜잭션
  SKIP LOCK으로 레코드 SELECT
  → status = 'PROCESSING' UPDATE
  → status_updated_at = NOW() UPDATE
  → 트랜잭션 커밋

[Phase 2] — 독립 트랜잭션
  payload JSON 파싱
  → 케이스 판별 (event_type)
  → 리필 건이고 created_at 기준 1분 초과: DB 잔량 반납 → SUCCESS 처리 → 종료
  → 그 외: Redis 동기화 메서드 재호출 (기존 Java 메서드 재사용)
     ├─ 성공(또는 멱등 방어 작동):
     │    status = 'SUCCESS', status_updated_at = NOW(), 트랜잭션 커밋
     └─ 실패 (Timeout 등):
          retry_count + 1, status = 'FAIL', status_updated_at = NOW(), 트랜잭션 커밋
```

### **6.4 리필 재시도 시 특수 처리 — 1분 경과 반납 로직**

```
스케줄러가 리필 Outbox 레코드 획득
  → created_at 기준 1분 초과 여부 확인
    ├─ 초과: Redis 재시도 없이 DB에 잔량 즉시 반납 → status = SUCCESS
    └─ 미초과: 정상 재시도 → UUID 멱등키 검증 포함
```

---

## **7. 주요 설계 원칙 요약**

| **영역** | **원칙** |
| --- | --- |
| Lua 스크립트 | Version 비교 + UPDATE를 단일 Lua 스크립트로 원자적 처리 |
| 멱등키 | 정책 수정 = Version(Epoch Second), 리필 = UUID + TTL 60s NX |
| Connection Failure | 정책·리필 모두 SUCCESS 기록 (정책: Hydration 위임, 리필: DB 반납) |
| Timeout | 항상 FAIL 기록 후 스케줄러 재시도 |
| PROCESSING 고착 | 5분 초과 시 재시도 대상 편입 |
| PENDING 격리 | 1분 이내 생성 건은 스케줄러에서 완전 제외 |
| 트랜잭션 분리 | Phase 1(선점) + Phase 2(실행) 으로 2단계 독립 처리 |
| 실행 환경 | `@Profile("traffic")` 전용, 다중 워커 SKIP LOCK으로 동시 실행 안전 |
| 재시도 실행 전략 | 전략 패턴(Strategy Pattern)으로 event_type별 독립 처리. if/switch 분기 없음 |

---

## **8. Outbox 재시도 전략 패턴 설계**

Outbox 재시도 스케줄러의 Phase 2에서 `event_type`별로 서로 다른 로직을 분기하기 위해 **전략 패턴(Strategy Pattern)**을 채택합니다.

### **8.1 설계 의도**

- `event_type`이 증가하더라도 기존 스케줄러 코드를 전혀 수정하지 않고, **새 전략 구현체 클래스 하나만 추가**하면 됩니다. (Open/Closed Principle)
- 각 전략은 자신의 로직만 담당하므로 단위 테스트 격리가 명확합니다.
- Spring DI가 모든 전략 구현체를 자동 수집(`List<OutboxEventRetryStrategy>`)하여 레지스트리로 관리합니다.

### **8.2 인터페이스 및 결과 정의**

```
OutboxEventRetryStrategy (interface)  ├── OutboxEventType supports()  │     // 이 전략이 담당하는 event_type 반환  └── OutboxRetryResult execute(RedisOutboxRecord record)        // 재시도 로직 수행 후 성공/실패 결과 반환OutboxRetryResult (enum)  ├── SUCCESS  // 정상 완료 또는 멱등 방어로 처리 불필요 판정  └── FAIL     // 재시도 가능한 오류 발생OutboxRetryStrategyRegistry (@Component)  // Spring이 주입한 List<OutboxEventRetryStrategy>를  // Map<OutboxEventType, OutboxEventRetryStrategy>로 빌드  // 스케줄러에서 O(1) 전략 조회
```

### **8.3 event_type별 전략 구현체 목록**

| **전략 클래스** | **담당 event_type** | **핵심 처리** |
| --- | --- | --- |
| `SyncPolicyActivationOutboxStrategy` | `SYNC_POLICY_ACTIVATION` | 정책 활성화 키 동기화 + version 비교 |
| `SyncLineLimitOutboxStrategy` | `SYNC_LINE_LIMIT` | 일 총량/공유 한도 키 동기화 + version 비교 |
| `SyncImmediateBlockOutboxStrategy` | `SYNC_IMMEDIATE_BLOCK` | 즉시차단 종료시각 키 동기화 + version 비교 |
| `SyncRepeatBlockOutboxStrategy` | `SYNC_REPEAT_BLOCK` | 반복차단 Hash 스냅샷 동기화 + 스냅샷 전체 version 비교 선행 |
| `SyncAppPolicyOutboxStrategy` | `SYNC_APP_POLICY` | 앱 정책 단건 동기화 + version 비교 |
| `SyncAppPolicySnapshotOutboxStrategy` | `SYNC_APP_POLICY_SNAPSHOT` | payload에서 lineId만 꺼내 DB 재조회 후 스냅샷 동기화 + version 비교 |
| `RefillOutboxStrategy` | `REFILL` | UUID 멱등키 검증(최우선) → 1분 초과 시 DB 반납 → 미초과 시 Redis 리필 |

---

## **9. 패키지 및 클래스 구성**

### **9.1 Domain (Outbox 핵심 타입)**

패키지: `com.pooli.traffic.domain.outbox`

| **클래스 / Enum** | **종류** | **역할** |
| --- | --- | --- |
| `RedisOutboxRecord` | Class | Outbox 테이블 레코드 매핑 도메인 객체 |
| `OutboxEventType` | Enum | `SYNC_POLICY_ACTIVATION`, `SYNC_LINE_LIMIT`, `SYNC_IMMEDIATE_BLOCK`, `SYNC_REPEAT_BLOCK`, `SYNC_APP_POLICY`, `SYNC_APP_POLICY_SNAPSHOT`, `REFILL` |
| `OutboxStatus` | Enum | `PENDING`, `PROCESSING`, `SUCCESS`, `FAIL` |
| `OutboxRetryResult` | Enum | `SUCCESS`, `FAIL` — 전략 실행 결과 반환 타입 |

### **9.2 Mapper (DB 접근)**

패키지: `com.pooli.traffic.mapper`

| **클래스** | **종류** | **역할** |
| --- | --- | --- |
| `RedisOutboxMapper` | Interface (MyBatis) | Outbox 레코드 삽입, 상태 갱신, SKIP LOCK 조회 쿼리 |

### **9.3 Service (Outbox 기록 & 스케줄러)**

패키지: `com.pooli.traffic.service.outbox`

| **클래스** | **종류** | **역할** |
| --- | --- | --- |
| `RedisOutboxRecordService` | @Service | Write-Through 직후 Outbox 레코드 삽입(PENDING), 결과에 따른 status 갱신 |
| `RedisOutboxRetryScheduler` | @Component | n초 주기 폴링. Phase 1(SKIP LOCK + PROCESSING 선점), Phase 2(전략 호출 + 결과 갱신) |

### **9.4 Strategy Pattern**

패키지: `com.pooli.traffic.service.outbox.strategy`

| **클래스 / Interface** | **종류** | **역할** |
| --- | --- | --- |
| `OutboxEventRetryStrategy` | Interface | `supports()` + `execute()` — 전략 계약 정의 |
| `OutboxRetryStrategyRegistry` | @Component | `List<OutboxEventRetryStrategy>` DI 수집 후 `Map<OutboxEventType, Strategy>` 관리 |
| `SyncPolicyActivationOutboxStrategy` | @Component | `SYNC_POLICY_ACTIVATION` 전략 구현 |
| `SyncLineLimitOutboxStrategy` | @Component | `SYNC_LINE_LIMIT` 전략 구현 |
| `SyncImmediateBlockOutboxStrategy` | @Component | `SYNC_IMMEDIATE_BLOCK` 전략 구현 |
| `SyncRepeatBlockOutboxStrategy` | @Component | `SYNC_REPEAT_BLOCK` 전략 구현 (스냅샷 전체 version 비교 선행) |
| `SyncAppPolicyOutboxStrategy` | @Component | `SYNC_APP_POLICY` 전략 구현 |
| `SyncAppPolicySnapshotOutboxStrategy` | @Component | `SYNC_APP_POLICY_SNAPSHOT` 전략 구현 (lineId로 DB 재조회 후 스냅샷 구성) |
| `RefillOutboxStrategy` | @Component | `REFILL` 전략 구현 (UUID 멱등키 최우선 검증, 1분 초과 시 DB 반납) |

---

## **10. 현재 코드베이스 동기화 현황**

> 분석 기준: 2026-03-15. 이 섹션은 기존 코드와의 충돌 여부 및 구현 전 결정 사항을 정리합니다.

### **10.1 미구현 항목 (전부 신규 추가)**

본 문서에 명시된 Outbox 관련 클래스는 현재 코드베이스에 **전혀 존재하지 않습니다.** 네이밍 충돌 없이 신규 구현하면 됩니다.

- `com.pooli.traffic.domain.outbox` 패키지 + 4종 클래스 (`RedisOutboxRecord`, `OutboxEventType`, `OutboxStatus`, `OutboxRetryResult`)
- `RedisOutboxMapper` (MyBatis)
- `RedisOutboxRecordService`, `RedisOutboxRetryScheduler`
- `com.pooli.traffic.service.outbox.strategy` 패키지 + 전략 인터페이스/레지스트리/구현체 9종

### **10.2 기존 코드 수정이 필요한 항목**

| **대상 클래스** | **필요 작업** |
| --- | --- |
| `TrafficRedisKeyFactory` | `refillIdempotencyKey(String uuid)` 메서드 추가 (§5.2 멱등키: `refill:idempotency:{uuid}`) |
| `TrafficRefillSourceMapper` | DB 반납 메서드 추가 (`restoreIndividualRemaining`, `restoreSharedRemaining`) — Connection Failure 시 및 1분 초과 반납 로직에서 사용 |
| Write-Through 호출 지점 (Policy 서비스 레이어) | Outbox 레코드 삽입(PENDING) 연동 추가 |

### **10.3 구현 전 설계 결정 필요 사항 ⚠️**

**Version Lua 스크립트 적용 범위:**

현재 `TrafficPolicyWriteThroughService`의 정책 동기화 메서드들은 단순 String/Hash/Set 직접 쓰기를 사용합니다 (version 비교 없음). §5.1의 version 기반 멱등성을 구현하려면 다음 중 선택이 필요합니다.

- **방안 A — Outbox Strategy 재시도 시에만 적용**: 최초 Write-Through는 현행 유지, 재시도 전략 내에서만 version Lua 사용. 구현이 단순하지만 최초 write-through에는 멱등성이 없음.
- **방안 B — Write-Through 메서드 전면 교체**: `policyKey`, `dailyTotalLimitKey`, `monthlySharedLimitKey` 등 현재 단순 String으로 저장된 키를 `{version, value}` Hash 구조로 변환. 완전한 멱등성을 보장하지만, **Lua 차감 스크립트(`executeDeductIndividual`, `executeDeductShared`)가 이 키들을 읽는 방식도 함께 수정**해야 합니다.